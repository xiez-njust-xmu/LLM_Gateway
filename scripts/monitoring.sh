#!/usr/bin/env bash
# 启动 / 停止 Prometheus + Grafana（原生二进制）
#
# 用法:
#   bash scripts/monitoring.sh start     # 启动两个服务
#   bash scripts/monitoring.sh stop
#   bash scripts/monitoring.sh restart
#   bash scripts/monitoring.sh status
#   bash scripts/monitoring.sh logs      # tail 日志
#   bash scripts/monitoring.sh targets   # 只渲染抓取配置并打印抓取目标
#   bash scripts/monitoring.sh reload    # 改了抓取目标后热加载（不重启 Prometheus）
#
# 抓取配置由 monitoring/prometheus.yml 渲染而来（写入 $MON_HOME/conf/prometheus.yml）：
# 同一 job 可以列多个 target，每个 target 自动带自己的 instance 标签，
# 所以多实例集群下 Grafana 看板无需改动，只是曲线/图例里多出几个 instance。
#
# 可覆盖的环境变量:
#   MON_HOME         二进制与数据目录（默认 $HOME/opt/monitoring）
#   PROM_PORT        Prometheus 端口（默认 9090）
#   GRAFANA_PORT     Grafana 端口（默认 3000）
#   GATEWAY_TARGETS  网关抓取目标，逗号分隔
#                    单实例（默认）: localhost:8000
#                    三实例集群    : localhost:8000,localhost:8001,localhost:8002
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MON_HOME="${MON_HOME:-$HOME/opt/monitoring}"
PROM_HOME="$MON_HOME/prometheus"
GRAFANA_HOME="$MON_HOME/grafana"
RUN_DIR="$MON_HOME/run"
LOG_DIR="$MON_HOME/logs"
PROM_PORT="${PROM_PORT:-9090}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
GATEWAY_TARGETS="${GATEWAY_TARGETS:-localhost:8000}"

PROM_PID_FILE="$RUN_DIR/prometheus.pid"
GRAFANA_PID_FILE="$RUN_DIR/grafana.pid"

log()  { printf '\033[1;34m[monitoring]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[monitoring]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[monitoring]\033[0m %s\n' "$*" >&2; exit 1; }

is_running() {  # $1 = pid 文件
    local f="$1" pid
    [ -s "$f" ] || return 1
    pid="$(cat "$f")"
    kill -0 "$pid" 2>/dev/null
}

stop_one() {  # $1 = pid 文件, $2 = 名称
    local f="$1" name="$2" pid
    if is_running "$f"; then
        pid="$(cat "$f")"
        log "停止 $name (pid=$pid)"
        kill "$pid" 2>/dev/null
        for _ in $(seq 1 20); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        kill -9 "$pid" 2>/dev/null
        rm -f "$f"
    else
        warn "$name 未在运行"
        rm -f "$f"
    fi
}

wait_http() {  # $1 = url, $2 = 超时秒数
    local url="$1" timeout="${2:-40}"
    for _ in $(seq 1 "$timeout"); do
        curl -sf -m 2 -o /dev/null "$url" && return 0
        sleep 1
    done
    return 1
}

# --- Grafana 配置渲染 -------------------------------------------------------
render_grafana_configs() {
    local prov="$GRAFANA_HOME/provisioning"
    mkdir -p "$prov/datasources" "$prov/dashboards"

    cp -f "$PROJECT_DIR/monitoring/grafana/provisioning/datasources/prometheus.yml" \
          "$prov/datasources/prometheus.yml"

    # Grafana 的 provisioning YAML 不展开环境变量，这里把绝对路径写死
    sed "s#__DASHBOARDS_PATH__#$PROJECT_DIR/monitoring/grafana/dashboards#" \
        "$PROJECT_DIR/monitoring/grafana/provisioning/dashboards/dashboards.yml.tpl" \
        > "$prov/dashboards/dashboards.yml"

    # 防呆：folderUid 若与仪表盘自身 uid 相同，仪表盘的 meta.folderUid 会指向自己，
    # Grafana 前端解析面包屑/权限时无限递归，页面报 Maximum call stack size exceeded。
    local folder_uid dash_uid f
    folder_uid="$(awk '/^[[:space:]]*folderUid:/{print $2; exit}' "$prov/dashboards/dashboards.yml")"
    if [ -n "$folder_uid" ]; then
        for f in "$PROJECT_DIR"/monitoring/grafana/dashboards/*.json; do
            [ -f "$f" ] || continue
            dash_uid="$(grep -oE '"uid"[[:space:]]*:[[:space:]]*"[^"]+"' "$f" | head -n 1 | sed -E 's/.*"([^"]+)"$/\1/')"
            if [ -n "$dash_uid" ] && [ "$dash_uid" = "$folder_uid" ]; then
                die "folderUid 与仪表盘 uid 都是 '$folder_uid'（$f），会导致 Grafana 页面栈溢出；请给 folderUid 换一个不同的值"
            fi
        done
    fi

    log "已渲染 Grafana provisioning -> $prov"
}

grafana_bin() {
    if [ -x "$GRAFANA_HOME/bin/grafana" ]; then
        printf '%s' "$GRAFANA_HOME/bin/grafana"
    else
        printf '%s' "$GRAFANA_HOME/bin/grafana-server"
    fi
}


# --- Prometheus 抓取配置渲染 --------------------------------------------------

# monitoring/prometheus.yml 是模板：把 ["__GATEWAY_TARGETS__"] 替换成本次要抓的实例列表。
# 单实例时渲染结果与原来完全一致（["localhost:8000"]），集群时变成多个 target。
render_prometheus_config() {
    local out="$MON_HOME/conf/prometheus.yml" targets="" t first=1
    mkdir -p "$MON_HOME/conf"
    for t in ${GATEWAY_TARGETS//,/ }; do
        [ "$first" = 1 ] && first=0 || targets+='", "'
        targets+="$t"
    done
    sed "s#__GATEWAY_TARGETS__#${targets}#" \
        "$PROJECT_DIR/monitoring/prometheus.yml" > "$out"
    log "已渲染抓取配置 -> $out"
}

cmd_targets() {
    render_prometheus_config
    echo "抓取目标（GATEWAY_TARGETS=$GATEWAY_TARGETS）:"
    awk '/job_name: llm-gateway/,/env: local/' "$MON_HOME/conf/prometheus.yml" | sed 's/^/  /'
    if curl -sf -m 3 -o /dev/null "http://localhost:$PROM_PORT/-/ready"; then
        echo "Prometheus 当前实际状态:"
        curl -s -m 5 "http://localhost:$PROM_PORT/api/v1/targets?state=active" \
            | tr ',' '\n' | grep -E '"scrapeUrl"|"health"' | sed 's/^/  /'
    fi
}

cmd_reload() {
    render_prometheus_config
    curl -sf -m 5 -X POST "http://localhost:$PROM_PORT/-/reload" \
        || die "reload 失败（Prometheus 未运行？需要 --web.enable-lifecycle，脚本启动时已带）"
    log "Prometheus 已热加载新配置（抓取目标: $GATEWAY_TARGETS）"
}

start_prometheus() {
    [ -x "$PROM_HOME/prometheus" ] || die "未找到 Prometheus，请先运行 bash scripts/install_monitoring.sh"
    if is_running "$PROM_PID_FILE"; then
        warn "Prometheus 已在运行 (pid=$(cat "$PROM_PID_FILE"))"
        return 0
    fi
    render_prometheus_config
    mkdir -p "$PROM_HOME/data" "$LOG_DIR" "$RUN_DIR"
    log "启动 Prometheus :$PROM_PORT"
    nohup "$PROM_HOME/prometheus" \
        --config.file="$MON_HOME/conf/prometheus.yml" \
        --storage.tsdb.path="$PROM_HOME/data" \
        --web.listen-address="0.0.0.0:$PROM_PORT" \
        --web.enable-lifecycle \
        > "$LOG_DIR/prometheus.log" 2>&1 &
    echo $! > "$PROM_PID_FILE"
    if wait_http "http://localhost:$PROM_PORT/-/ready" 40; then
        log "Prometheus 就绪: http://localhost:$PROM_PORT"
    else
        warn "Prometheus 启动超时，请查看 $LOG_DIR/prometheus.log"
    fi
}

start_grafana() {
    local bin; bin="$(grafana_bin)"
    [ -x "$bin" ] || die "未找到 Grafana，请先运行 bash scripts/install_monitoring.sh"
    if is_running "$GRAFANA_PID_FILE"; then
        warn "Grafana 已在运行 (pid=$(cat "$GRAFANA_PID_FILE"))"
        return 0
    fi

    render_grafana_configs
    mkdir -p "$GRAFANA_HOME/data" "$GRAFANA_HOME/logs" "$GRAFANA_HOME/plugins" "$RUN_DIR"

    export GF_PATHS_HOME="$GRAFANA_HOME"
    export GF_PATHS_DATA="$GRAFANA_HOME/data"
    export GF_PATHS_LOGS="$GRAFANA_HOME/logs"
    export GF_PATHS_PLUGINS="$GRAFANA_HOME/plugins"
    export GF_PATHS_PROVISIONING="$GRAFANA_HOME/provisioning"
    export GF_SERVER_HTTP_ADDR="0.0.0.0"
    export GF_SERVER_HTTP_PORT="$GRAFANA_PORT"
    export GF_SECURITY_ADMIN_USER="admin"
    export GF_SECURITY_ADMIN_PASSWORD="admin"
    export GF_AUTH_ANONYMOUS_ENABLED="true"
    export GF_AUTH_ANONYMOUS_ORG_ROLE="Viewer"
    export GF_ANALYTICS_REPORTING_ENABLED="false"
    export GF_ANALYTICS_CHECK_FOR_UPDATES="false"
    export GF_NEWS_NEWS_FEED_ENABLED="false"

    log "启动 Grafana :$GRAFANA_PORT"
    if [ "$(basename "$bin")" = "grafana" ]; then
        nohup "$bin" server --homepath "$GRAFANA_HOME" > "$LOG_DIR/grafana.log" 2>&1 &
    else
        nohup "$bin" --homepath "$GRAFANA_HOME" > "$LOG_DIR/grafana.log" 2>&1 &
    fi
    echo $! > "$GRAFANA_PID_FILE"
    if wait_http "http://localhost:$GRAFANA_PORT/api/health" 60; then
        log "Grafana 就绪: http://localhost:$GRAFANA_PORT (admin/admin，匿名可只读查看)"
    else
        warn "Grafana 启动超时，请查看 $LOG_DIR/grafana.log"
    fi
}


cmd_start() {
    start_prometheus
    start_grafana
    echo
    cmd_status
    echo
    log "仪表盘: http://localhost:$GRAFANA_PORT/d/llm-gateway"
    log "抓取目标: http://localhost:$PROM_PORT/targets"
}

cmd_stop() {
    stop_one "$GRAFANA_PID_FILE" "Grafana"
    stop_one "$PROM_PID_FILE" "Prometheus"
}

cmd_status() {
    echo "--- 服务状态 ---"
    if is_running "$PROM_PID_FILE"; then
        echo "Prometheus : pid=$(cat "$PROM_PID_FILE")  http://localhost:$PROM_PORT"
        curl -s -m 3 "http://localhost:$PROM_PORT/-/ready" && echo || echo "  (未就绪)"
    else
        echo "Prometheus : 未运行"
    fi
    if is_running "$GRAFANA_PID_FILE"; then
        echo "Grafana    : pid=$(cat "$GRAFANA_PID_FILE")  http://localhost:$GRAFANA_PORT"
        curl -s -m 3 "http://localhost:$GRAFANA_PORT/api/health" && echo || echo "  (未就绪)"
    else
        echo "Grafana    : 未运行"
    fi
    echo "--- 网关状态 ---"
    curl -s -m 3 "http://localhost:8000/health" && echo || echo "  (网关未运行?)"
}

cmd_logs() {
    tail -n 30 "$LOG_DIR/prometheus.log" 2>/dev/null || warn "无 prometheus.log"
    echo "----------------------------------------"
    tail -n 30 "$LOG_DIR/grafana.log" 2>/dev/null || warn "无 grafana.log"
}

case "${1:-start}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_stop; sleep 1; cmd_start ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    targets) cmd_targets ;;
    reload)  cmd_reload ;;
    *)       die "未知命令: $1（可用: start|stop|restart|status|logs|targets|reload）" ;;
esac