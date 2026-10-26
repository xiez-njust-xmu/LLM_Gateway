#!/usr/bin/env bash
# 多实例网关集群：一台机器上起 N 个网关进程，前面挂 nginx 做负载均衡
#
# 为什么能直接水平扩？——网关进程本身无状态，状态全在外部组件里：
#   * 限流额度：Redis（3 个实例共享同一份额度，不会各自放行 10 次）
#   * 请求历史：PostgreSQL（推荐）/ SQLite（单机演示可跑，多实例写同一文件有锁竞争）
#   * 事件总线：Kafka
#   * 指标：每个实例各自暴露 /metrics，由 Prometheus 直连抓取（不要经过 LB）
# 每个实例只有「端口 + 实例名 + 自己的日志文件」不同，其余配置完全一致。
#
# 用法:
#   bash scripts/cluster.sh start        # 起 N 个实例，并接上 nginx
#   bash scripts/cluster.sh stop
#   bash scripts/cluster.sh restart
#   bash scripts/cluster.sh status
#   bash scripts/cluster.sh logs
#   bash scripts/cluster.sh scale 5      # 扩到 5 个实例并热加载 nginx（滚动，不断连接）
#
# 可覆盖的环境变量:
#   CLUSTER_SIZE     实例数（默认 3）
#   BASE_PORT        起始端口（默认 8000，实例依次用 8000 / 8001 / 8002…）
#   INSTANCE_PREFIX  实例名前缀（默认 gateway，实例名 gateway-8000）
#   PYTHON           解释器（默认 python3）
#   WITH_NGINX       1=同时管理 nginx（默认 1），0=只起实例自己接别的负载均衡
#   REDIS_URL / DATABASE_URL / KAFKA_BOOTSTRAP / RATE_LIMIT_BACKEND 同单实例
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_SIZE="${CLUSTER_SIZE:-3}"
BASE_PORT="${BASE_PORT:-8000}"
INSTANCE_PREFIX="${INSTANCE_PREFIX:-gateway}"
PYTHON="${PYTHON:-python3}"
WITH_NGINX="${WITH_NGINX:-1}"
PG_PORT="${PG_PORT:-5433}"

RUN_DIR="$PROJECT_DIR/logs/cluster/run"
LOG_DIR="$PROJECT_DIR/logs/cluster"

log()  { printf '\033[1;34m[cluster]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[cluster]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[cluster]\033[0m %s\n' "$*" >&2; exit 1; }

mkdir -p "$RUN_DIR" "$LOG_DIR"

is_running() {  # $1 = pid 文件
    local f="$1" pid
    [ -s "$f" ] || return 1
    pid="$(cat "$f")"
    kill -0 "$pid" 2>/dev/null
}

port_open() {  # $1 = 端口
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&- ; return 0; }
    return 1
}

wait_health() {  # $1 = 端口, $2 = 超时秒数
    local port="$1" timeout="${2:-30}"
    for _ in $(seq 1 "$timeout"); do
        curl -sf -m 2 -o /dev/null "http://127.0.0.1:$port/health" && return 0
        sleep 1
    done
    return 1
}

# --- 集群参数 ---------------------------------------------------------------

instance_ports() {   # 输出本脚本管辖的端口（0..CLUSTER_SIZE-1）
    local i
    for (( i = 0; i < CLUSTER_SIZE; i++ )); do
        printf '%s\n' "$((BASE_PORT + i))"
    done
}

instance_list() {    # 输出 nginx 上游列表：127.0.0.1:8000,127.0.0.1:8001,...
    local ports; ports="$(instance_ports)"
    printf '%s' "$(printf '127.0.0.1:%s,' $ports | sed 's/,$//')"
}

# 实际正在运行的实例端口（按 pid 文件推断）。
# 不能只信环境变量 CLUSTER_SIZE：每次执行脚本都是新进程，
# `scale 5` 之后再来一次 `scale 3`，得先知道现在到底跑着哪几个。
running_ports() {
    local f p
    for f in "$RUN_DIR"/gateway-*.pid; do
        [ -f "$f" ] || continue
        p="${f##*/gateway-}"; p="${p%.pid}"
        is_running "$f" && printf '%s\n' "$p"
    done | sort -n
}

# 配置的端口 ∪ 实际在跑的端口（status/logs 用这个，避免看不见「多出来的实例」）
all_ports() {
    { instance_ports; running_ports; } | sort -n -u
}

pid_file() { printf '%s/gateway-%s.pid' "$RUN_DIR" "$1"; }
log_file() { printf '%s/gateway-%s.log'  "$LOG_DIR" "$1"; }

default_database_url() {
    if [ -n "${DATABASE_URL:-}" ]; then
        printf '%s' "$DATABASE_URL"; return
    fi
    if port_open "$PG_PORT"; then
        printf 'postgresql://gw:gw@localhost:%s/gateway' "$PG_PORT"
    else
        printf 'sqlite:///%s/logs/gateway.db' "$PROJECT_DIR"
    fi
}

# --- 实例启停 ---------------------------------------------------------------

start_instance() {   # $1 = 端口
    local port="$1" pid_f log_f
    pid_f="$(pid_file "$port")"; log_f="$(log_file "$port")"
    if is_running "$pid_f"; then
        warn "实例 :$port 已在运行 (pid=$(cat "$pid_f"))"
        return 0
    fi
    if port_open "$port"; then
        warn "端口 $port 已被别的进程占用，跳过该实例（排查看: ss -ltnp | grep :$port）"
        return 1
    fi
    (
        cd "$PROJECT_DIR" || exit 1
        SERVER_HOST=0.0.0.0 \
        SERVER_PORT="$port" \
        GATEWAY_INSTANCE="$INSTANCE_PREFIX-$port" \
        GATEWAY_LOG_FILE="$log_f" \
        REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}" \
        DATABASE_URL="$DATABASE_URL_RESOLVED" \
        KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}" \
        RATE_LIMIT_BACKEND="${RATE_LIMIT_BACKEND:-auto}" \
        nohup "$PYTHON" main.py >> "$log_f" 2>&1 &
        echo $! > "$pid_f"
    )
    local pid; pid="$(cat "$pid_f" 2>/dev/null)"
    if wait_health "$port" 40; then
        log "实例 $INSTANCE_PREFIX-$port 就绪 (pid=$pid, 日志 $log_f)"
        return 0
    fi
    warn "实例 :$port 启动超时，见 $log_f"
    return 1
}

stop_instance() {    # $1 = 端口
    local port="$1" pid_f pid
    pid_f="$(pid_file "$port")"
    if ! is_running "$pid_f"; then
        rm -f "$pid_f"
        return 0
    fi
    pid="$(cat "$pid_f")"
    log "停止实例 :$port (pid=$pid)"
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    kill -9 "$pid" 2>/dev/null
    rm -f "$pid_f"
}

# --- nginx 接入 -------------------------------------------------------------

nginx_start()  { GATEWAY_INSTANCES="$(instance_list)" bash "$PROJECT_DIR/scripts/nginx.sh" start; }
nginx_stop()   { bash "$PROJECT_DIR/scripts/nginx.sh" stop; }
nginx_reload() { GATEWAY_INSTANCES="$(instance_list)" bash "$PROJECT_DIR/scripts/nginx.sh" reload; }

check_infra_hints() {
    port_open 6379     || warn "Redis(6379) 未启动：各实例退化成「进程内限流」，总额度会变成 N 倍（bash scripts/infra.sh start）"
    port_open "$PG_PORT" || warn "PostgreSQL($PG_PORT) 未启动：历史写入退化为 SQLite 单文件"
    port_open 9092     || warn "Kafka(9092) 未启动：事件将降级写 logs/events.jsonl"
}

# --- 命令 -------------------------------------------------------------------

cmd_start() {
    DATABASE_URL_RESOLVED="$(default_database_url)"
    case "$DATABASE_URL_RESOLVED" in
        sqlite:*) warn "多实例共用 SQLite（$DATABASE_URL_RESOLVED）：演示够用，生产请换 PostgreSQL" ;;
    esac
    check_infra_hints

    log "启动 $CLUSTER_SIZE 个实例: 端口 $(instance_ports | tr '\n' ' ')"
    local started=0 port
    for port in $(instance_ports); do
        start_instance "$port" && started=$((started + 1))
    done
    [ "$started" -gt 0 ] || die "没有任何实例启动成功"

    [ "$WITH_NGINX" = "1" ] && nginx_start
    echo
    cmd_status
}

cmd_stop() {
    [ "$WITH_NGINX" = "1" ] && nginx_stop
    local port
    for port in $(running_ports); do
        stop_instance "$port"
    done
    # 配置里但没跑起来的，清掉残留 pid 文件
    for port in $(instance_ports); do
        rm -f "$(pid_file "$port")"
    done
}

cmd_status() {
    echo "--- 网关实例（配置 $CLUSTER_SIZE 个: $(instance_ports | tr '\n' ' ')，实跑: $(running_ports | tr '\n' ' ')）---"
    local port pid_f
    for port in $(all_ports); do
        pid_f="$(pid_file "$port")"
        if is_running "$pid_f"; then
            printf '  :%s pid=%-7s ' "$port" "$(cat "$pid_f")"
            curl -s -m 5 "http://127.0.0.1:$port/health" || echo "(无响应)"
            echo
        else
            printf '  :%s 未运行（日志 %s）\n' "$port" "$(log_file "$port")"
        fi
    done
    echo "--- 负载均衡（nginx :${LB_PORT:-8080}）---"
    if [ "$WITH_NGINX" = "1" ]; then
        LB_PORT="${LB_PORT:-8080}" GATEWAY_INSTANCES="$(instance_list)" \
            bash "$PROJECT_DIR/scripts/nginx.sh" status
    else
        echo "  已关闭 nginx 管理（WITH_NGINX=0），上游列表: $(instance_list)"
    fi
}

cmd_logs() {
    local port f
    for port in $(all_ports); do
        f="$(log_file "$port")"
        echo "===== gateway-$port.log（末尾 8 行）====="
        tail -n 8 "$f" 2>/dev/null || warn "无 $f"
    done
    echo "===== nginx 访问日志（末尾 5 行）====="
    tail -n 5 "${NGINX_HOME:-$HOME/opt/nginx}/logs/nginx-access.log" 2>/dev/null \
        || warn "无 nginx 访问日志"
}

cmd_scale() {   # $1 = 目标实例数
    local target="${1:-}"
    case "$target" in
        ''|*[!0-9]*) die "用法: bash scripts/cluster.sh scale <实例数>，例如 scale 5" ;;
    esac
    [ "$target" -ge 1 ] || die "实例数至少为 1"

    local old_ports new_ports p
    old_ports=" $(running_ports | tr '\n' ' ')"
    new_ports=" $(CLUSTER_SIZE="$target" instance_ports | tr '\n' ' ')"
    DATABASE_URL_RESOLVED="$(default_database_url)"

    # 扩容：先把新实例起好（同样先过一遍基础设施检查）
    if [ "$(echo "$new_ports" | wc -w)" -gt "$(echo "$old_ports" | wc -w)" ]; then
        check_infra_hints
    fi
    for p in $new_ports; do
        case "$old_ports" in *" $p "*) continue ;; esac
        start_instance "$p"
    done

    # 改上游列表并热加载：新增的实例开始接流量，要摘掉的实例先不再接新请求
    CLUSTER_SIZE="$target"
    [ "$WITH_NGINX" = "1" ] && nginx_reload

    # 缩容：reload 之后剩下的在途请求处理完，再停实例
    for p in $old_ports; do
        case "$new_ports" in *" $p "*) continue ;; esac
        stop_instance "$p"
    done

    log "实例数已调整为 $target（上游 $(instance_list)）"
    echo
    cmd_status
}

case "${1:-status}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_stop; sleep 1; cmd_start ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    scale)   cmd_scale "${2:-}" ;;
    *)       die "未知命令: $1（可用: start|stop|restart|status|logs|scale <N>）" ;;
esac
