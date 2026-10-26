#!/usr/bin/env bash
# nginx 负载均衡层：多实例网关的统一入口
#
# 本机既没有 root（sudo 要密码）也没有 Docker，所以 nginx 同样走「用户态本地部署」：
#   * apt-get download 只拉 nginx 的 deb 包，dpkg-deb -x 解包到 $NGINX_HOME（这个 deb 是
#     纯静态二进制，ldd 检查过系统库齐全），不需要写 /usr/sbin；
#   * 配置由 nginx/nginx.conf.tpl 渲染，所有日志 / pid / 临时目录都落在 $NGINX_HOME 下，
#     监听端口默认 8080（>1024，非 root 可直接绑定）。
#
# 用法:
#   bash scripts/nginx.sh install    # 首次：下载并解包 nginx（幂等）
#   bash scripts/nginx.sh start      # 渲染配置 + 校验 + 启动（上游端口由 GATEWAY_INSTANCES 决定）
#   bash scripts/nginx.sh stop
#   bash scripts/nginx.sh restart
#   bash scripts/nginx.sh reload     # 改完模板/增删实例后热加载（不断连接）
#   bash scripts/nginx.sh test       # 只做 nginx -t 配置校验
#   bash scripts/nginx.sh status
#   bash scripts/nginx.sh logs
#
# 可覆盖的环境变量:
#   NGINX_HOME        二进制 / 配置 / 日志目录（默认 $HOME/opt/nginx）
#   LB_PORT           负载均衡监听端口（默认 8080）
#   GATEWAY_INSTANCES 上游列表，逗号分隔（默认 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002）
#   NGINX_MAX_FAILS   摘除实例前允许的失败次数（默认 2）
#   NGINX_FAIL_TIMEOUT 摘除后多久再试（默认 10s）
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NGINX_HOME="${NGINX_HOME:-$HOME/opt/nginx}"
NGINX_BIN="$NGINX_HOME/bin/nginx"
NGINX_CONF="$NGINX_HOME/conf/nginx.conf"
NGINX_TEMPLATE="$PROJECT_DIR/nginx/nginx.conf.tpl"
LOG_DIR="$NGINX_HOME/logs"
RUN_DIR="$NGINX_HOME/run"
TMP_DIR="$NGINX_HOME/tmp"
NGINX_PID="$RUN_DIR/nginx.pid"

LB_PORT="${LB_PORT:-8080}"
GATEWAY_INSTANCES="${GATEWAY_INSTANCES:-127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002}"
NGINX_MAX_FAILS="${NGINX_MAX_FAILS:-2}"
NGINX_FAIL_TIMEOUT="${NGINX_FAIL_TIMEOUT:-10s}"

log()  { printf '\033[1;34m[nginx]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[nginx]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[nginx]\033[0m %s\n' "$*" >&2; exit 1; }

mkdir -p "$LOG_DIR" "$RUN_DIR" "$TMP_DIR" "$NGINX_HOME/conf"

is_running() {  # $1 = pid 文件
    local f="$1" pid
    [ -s "$f" ] || return 1
    pid="$(cat "$f")"
    kill -0 "$pid" 2>/dev/null
}

port_open() {  # $1 = 端口；本地端口能否连上（上游存活探测）
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&- ; return 0; }
    return 1
}

wait_http() {  # $1 = url, $2 = 超时秒数
    local url="$1" timeout="${2:-20}"
    for _ in $(seq 1 "$timeout"); do
        curl -sf -m 2 -o /dev/null "$url" && return 0
        sleep 1
    done
    return 1
}

# --- 安装（下载 + 解包，全部落到 $NGINX_HOME，不需要 root）---------------------

install_nginx() {
    if [ -x "$NGINX_BIN" ]; then
        log "nginx 已就绪: $("$NGINX_BIN" -v 2>&1)"
        return 0
    fi
    local deb_dir="$NGINX_HOME/downloads" root="$NGINX_HOME/root"
    mkdir -p "$deb_dir" "$NGINX_HOME/bin"
    if ! ls "$deb_dir"/nginx_*.deb >/dev/null 2>&1; then
        log "apt-get download nginx -> $deb_dir"
        ( cd "$deb_dir" && apt-get download nginx >/dev/null 2>&1 ) \
            || die "apt-get download nginx 失败（检查网络或代理）"
    fi
    rm -rf "$root"; mkdir -p "$root"
    local deb
    for deb in "$deb_dir"/nginx_*.deb; do
        [ -f "$deb" ] || continue
        dpkg-deb -x "$deb" "$root" || die "解包 $deb 失败"
    done
    [ -x "$root/usr/sbin/nginx" ] || die "deb 里没有 usr/sbin/nginx，请检查包内容"
    cp -a "$root/usr/sbin/nginx" "$NGINX_BIN" || die "复制 nginx 二进制失败"
    rm -rf "$root"
    if ldd "$NGINX_BIN" 2>/dev/null | grep -qi 'not found'; then
        warn "nginx 存在缺失的动态库（下面这些通常由系统包提供）:"
        ldd "$NGINX_BIN" | grep -i 'not found'
    fi
    log "nginx 安装完成: $("$NGINX_BIN" -v 2>&1)  ($NGINX_BIN)"
}

# --- 配置渲染 ---------------------------------------------------------------

render_config() {
    [ -f "$NGINX_TEMPLATE" ] || die "缺少模板 $NGINX_TEMPLATE"
    mkdir -p "$TMP_DIR"/client_body "$TMP_DIR"/proxy "$TMP_DIR"/fastcgi \
             "$TMP_DIR"/uwsgi "$TMP_DIR"/scgi

    # 上游列表 -> 多行 server 指令（每个实例带被动健康检查参数）
    local servers="" addr n=0
    local -a addrs
    IFS=',' read -ra addrs <<< "$GATEWAY_INSTANCES"
    for addr in "${addrs[@]}"; do
        addr="${addr//[[:space:]]/}"
        [ -n "$addr" ] || continue
        servers+="        server $addr max_fails=$NGINX_MAX_FAILS fail_timeout=$NGINX_FAIL_TIMEOUT;"$'\n'
        n=$((n + 1))
    done
    [ "$n" -gt 0 ] || die "GATEWAY_INSTANCES 为空，示例: GATEWAY_INSTANCES=127.0.0.1:8000,127.0.0.1:8001"

    awk -v s="$servers" '{ if ($0 ~ /__UPSTREAM_SERVERS__/) printf "%s", s; else print }' \
        "$NGINX_TEMPLATE" \
        | sed -e "s#__LB_PORT__#$LB_PORT#g" \
              -e "s#__LOG_DIR__#$LOG_DIR#g" \
              -e "s#__RUN_DIR__#$RUN_DIR#g" \
              -e "s#__TMP_DIR__#$TMP_DIR#g" \
              -e "s#__PID_FILE__#$NGINX_PID#g" \
              -e "s#__NGINX_HOME__#$NGINX_HOME#g" \
        > "$NGINX_CONF"
    log "已渲染配置 -> $NGINX_CONF（$n 个上游: $GATEWAY_INSTANCES）"
}

nginx_test() {
    [ -x "$NGINX_BIN" ] || die "未找到 nginx，请先运行 bash scripts/nginx.sh install"
    [ -f "$NGINX_CONF" ] || render_config
    "$NGINX_BIN" -p "$NGINX_HOME" -c "$NGINX_CONF" -t
}

# --- 启停 -------------------------------------------------------------------

start_nginx() {
    [ -x "$NGINX_BIN" ] || die "未找到 nginx，请先运行 bash scripts/nginx.sh install"
    if is_running "$NGINX_PID"; then
        warn "nginx 已在运行 (pid=$(cat "$NGINX_PID"))；改配置请用 reload"
        return 0
    fi
    render_config
    if ! "$NGINX_BIN" -p "$NGINX_HOME" -c "$NGINX_CONF" -t > "$LOG_DIR/nginx-test.log" 2>&1; then
        cat "$LOG_DIR/nginx-test.log" >&2
        die "nginx 配置校验失败（完整输出见 $LOG_DIR/nginx-test.log）"
    fi

    # 上游存活先提示（不阻断启动：实例可以稍后再起，起来后 nginx 自动加回）
    local addr port
    for addr in ${GATEWAY_INSTANCES//,/ }; do
        port="${addr##*:}"
        port_open "$port" || warn "上游 $addr 当前连不上（请求会被分到其它实例）"
    done

    log "启动 nginx 负载均衡 :$LB_PORT"
    "$NGINX_BIN" -p "$NGINX_HOME" -c "$NGINX_CONF" > "$LOG_DIR/nginx-start.log" 2>&1 \
        || die "nginx 启动失败，见 $LOG_DIR/nginx-start.log"

    if wait_http "http://127.0.0.1:$LB_PORT/lb-health" 10; then
        log "负载均衡就绪: http://localhost:$LB_PORT  (健康检查 /lb-health)"
    else
        warn "nginx 未就绪，见 $LOG_DIR/nginx-error.log"
    fi
}

stop_nginx() {
    if ! is_running "$NGINX_PID"; then
        warn "nginx 未在运行"
        rm -f "$NGINX_PID"
        return 0
    fi
    local pid; pid="$(cat "$NGINX_PID")"
    log "优雅停止 nginx (pid=$pid)"
    "$NGINX_BIN" -p "$NGINX_HOME" -c "$NGINX_CONF" -s quit 2>/dev/null || kill "$pid" 2>/dev/null
    for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    kill -9 "$pid" 2>/dev/null
    rm -f "$NGINX_PID"
}

reload_nginx() {
    is_running "$NGINX_PID" || die "nginx 未运行，请先 start"
    render_config
    nginx_test || die "配置有误，已放弃 reload（旧配置继续生效）"
    "$NGINX_BIN" -p "$NGINX_HOME" -c "$NGINX_CONF" -s reload \
        || die "reload 失败，见 $LOG_DIR/nginx-error.log"
    log "已热加载（worker 平滑重启，连接不中断）"
}

# --- 状态 -------------------------------------------------------------------

# 经 LB 打几次 /health，看响应体里的 instance 字段（最直观的分发验证）
sample_distribution() {
    local n="${1:-6}" i inst
    echo "--- 经 LB 请求 $n 次的实例分布 ---"
    for i in $(seq 1 "$n"); do
        inst="$(curl -s -m 5 "http://127.0.0.1:$LB_PORT/health" \
                | sed -n 's/.*"instance":"\([^"]*\)".*/\1/p')"
        printf '  #%s -> %s\n' "$i" "${inst:-无响应}"
    done
}

cmd_status() {
    echo "--- nginx ---"
    if is_running "$NGINX_PID"; then
        echo "nginx      : pid=$(cat "$NGINX_PID")  监听 :$LB_PORT  ($NGINX_HOME)"
        nginx_test 2>&1 | sed 's/^/             /'
    else
        echo "nginx      : 未运行"
    fi
    echo "--- 上游实例 ---"
    local addr port
    for addr in ${GATEWAY_INSTANCES//,/ }; do
        port="${addr##*:}"
        if port_open "$port"; then
            printf '  %-18s 存活  ' "$addr"
            curl -s -m 5 "http://$addr/health" && echo || echo "(无响应)"
        else
            printf '  %-18s 不可达\n' "$addr"
        fi
    done
    echo "--- 负载均衡入口 ---"
    curl -s -m 3 "http://127.0.0.1:$LB_PORT/lb-health" || echo "  (LB 未运行?)"
    is_running "$NGINX_PID" && sample_distribution 6
    echo "--- 日志 ---"
    echo "  访问日志: $LOG_DIR/nginx-access.log"
    echo "  错误日志: $LOG_DIR/nginx-error.log"
}

cmd_logs() {
    echo "===== nginx-access.log（末尾 15 行，含 upstream_addr / instance）====="
    tail -n 15 "$LOG_DIR/nginx-access.log" 2>/dev/null || warn "无 $LOG_DIR/nginx-access.log"
    echo "===== nginx-error.log（末尾 15 行）====="
    tail -n 15 "$LOG_DIR/nginx-error.log" 2>/dev/null || warn "无 $LOG_DIR/nginx-error.log"
}

case "${1:-status}" in
    install) install_nginx ;;
    start)   install_nginx; start_nginx ;;
    stop)    stop_nginx ;;
    restart) stop_nginx; sleep 1; install_nginx; start_nginx ;;
    reload)  reload_nginx ;;
    test)    install_nginx; render_config; nginx_test ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    *)       die "未知命令: $1（可用: install|start|stop|restart|reload|test|status|logs）" ;;
esac
