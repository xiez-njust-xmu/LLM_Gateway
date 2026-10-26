#!/usr/bin/env bash
# 本地基础设施：Redis（分布式限流）、PostgreSQL（请求历史）、Kafka（异步事件）
#
# 本机既没有 root（sudo 要密码）也没有 Docker，因此这里走「用户态本地部署」：
#   * Redis / PostgreSQL：apt-get download 只拉 deb 包，再用 dpkg-deb -x 解包到
#     $INFRA_HOME，完全不需要写系统目录（Redis 唯一的动态依赖 liblzf 一起解包）；
#   * Kafka：下载官方 tarball，用 KRaft 模式跑单节点（broker + controller 同进程），
#     只需系统已有 java（本机 Java 25）。
#
# 用法:
#   bash scripts/infra.sh install    # 首次：下载并解包三件套（幂等，已存在则跳过）
#   bash scripts/infra.sh start      # 启动三个服务（含首次初始化数据目录 / 建库 / 建 topic）
#   bash scripts/infra.sh stop
#   bash scripts/infra.sh restart
#   bash scripts/infra.sh status
#   bash scripts/infra.sh logs
#
# 可覆盖的环境变量:
#   INFRA_HOME  二进制与数据目录（默认 $HOME/opt/infra）
#   REDIS_PORT  默认 6379
#   PG_PORT     默认 5433（避开系统可能存在的 5432）
#   KAFKA_PORT  默认 9092
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_HOME="${INFRA_HOME:-$HOME/opt/infra}"
DOWNLOAD_DIR="$INFRA_HOME/downloads"
RUN_DIR="$INFRA_HOME/run"
LOG_DIR="$INFRA_HOME/logs"

REDIS_PORT="${REDIS_PORT:-6379}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-gw}"
PG_PASSWORD="${PG_PASSWORD:-gw}"
PG_DB="${PG_DB:-gateway}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
KAFKA_CONTROLLER_PORT="${KAFKA_CONTROLLER_PORT:-9093}"
KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
KAFKA_SCALA="${KAFKA_SCALA:-2.13}"
KAFKA_TOPIC="${KAFKA_TOPIC:-llm-gateway-events}"

REDIS_BIN="$INFRA_HOME/redis/bin/redis-server"
REDIS_CLI="$INFRA_HOME/redis/bin/redis-cli"
REDIS_LIB="$INFRA_HOME/redis/lib"
PG_ROOT="$INFRA_HOME/postgres"
PGBIN="$PG_ROOT/usr/lib/postgresql/18/bin"
PG_LIB="$PG_ROOT/usr/lib/x86_64-linux-gnu"
PGDATA="$INFRA_HOME/pgdata"
KAFKA_HOME="$INFRA_HOME/kafka/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}"
KAFKA_DATA="$INFRA_HOME/kafka-data"

REDIS_PID="$RUN_DIR/redis.pid"
KAFKA_PID="$RUN_DIR/kafka.pid"

log()  { printf '\033[1;34m[infra]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[infra]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[infra]\033[0m %s\n' "$*" >&2; exit 1; }

mkdir -p "$DOWNLOAD_DIR" "$RUN_DIR" "$LOG_DIR"

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
    else
        warn "$name 未在运行"
    fi
    rm -f "$f"
}

wait_port() {  # $1 = 端口, $2 = 超时秒数
    local port="$1" timeout="${2:-20}"
    for _ in $(seq 1 "$timeout"); do
        (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && { exec 3>&- ; return 0; }
        sleep 1
    done
    return 1
}

# --- 安装（下载 + 解包，全部落到 $INFRA_HOME，不需要 root）-------------------

install_redis() {
    if [ -x "$REDIS_BIN" ]; then
        log "Redis 已就绪: $REDIS_BIN"
        return 0
    fi
    log "解包 Redis（apt-get download + dpkg-deb -x）"
    local deb_dir="$DOWNLOAD_DIR/redis" root
    mkdir -p "$deb_dir" "$INFRA_HOME/redis/bin" "$INFRA_HOME/redis/lib"
    ( cd "$deb_dir" && apt-get download redis-server redis-tools liblzf1 >/dev/null 2>&1 ) \
        || warn "apt-get download 失败（离线环境可手工放 deb 到 $deb_dir）"
    root="$(mktemp -d)"
    for d in "$deb_dir"/*.deb; do
        [ -f "$d" ] || continue
        dpkg-deb -x "$d" "$root"
    done
    cp -a "$root"/usr/bin/redis-server "$root"/usr/bin/redis-cli "$INFRA_HOME/redis/bin/" 2>/dev/null
    cp -a "$root"/usr/lib/x86_64-linux-gnu/liblzf.so* "$INFRA_HOME/redis/lib/" 2>/dev/null
    rm -rf "$root"
    [ -x "$REDIS_BIN" ] || die "Redis 解包失败，请检查 $deb_dir"
    log "Redis 解包完成: $("$REDIS_BIN" --version 2>/dev/null || echo "$REDIS_BIN")"
}

install_postgres() {
    if [ -x "$PGBIN/postgres" ]; then
        log "PostgreSQL 已就绪: $PGBIN"
        return 0
    fi
    log "解包 PostgreSQL（apt-get download + dpkg-deb -x，deb 目录结构原样保留，"
    log "这样 postgres 二进制能按相对路径找到 share/postgresql）"
    local deb_dir="$DOWNLOAD_DIR/postgres" root
    mkdir -p "$deb_dir" "$PG_ROOT"
    ( cd "$deb_dir" && apt-get download postgresql-18 postgresql-client-18 libpq5 >/dev/null 2>&1 ) \
        || warn "apt-get download 失败（离线环境可手工放 deb 到 $deb_dir）"
    for d in "$deb_dir"/*.deb; do
        [ -f "$d" ] || continue
        dpkg-deb -x "$d" "$PG_ROOT"
    done
    [ -x "$PGBIN/postgres" ] || die "PostgreSQL 解包失败，请检查 $deb_dir"
    log "PostgreSQL 解包完成: $("$PGBIN/postgres" --version)"
}

install_kafka() {
    if [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
        log "Kafka 已就绪: $KAFKA_HOME"
        return 0
    fi
    local tgz="$DOWNLOAD_DIR/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
    if [ ! -s "$tgz" ]; then
        log "下载 Kafka ${KAFKA_VERSION} -> $tgz"
        curl -fL --retry 2 -o "$tgz" \
            "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz" \
            || die "Kafka 下载失败"
    fi
    log "解包 Kafka 到 $INFRA_HOME/kafka"
    mkdir -p "$INFRA_HOME/kafka"
    tar -xzf "$tgz" -C "$INFRA_HOME/kafka"
    [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ] || die "Kafka 解包失败（检查 $INFRA_HOME/kafka）"
    log "Kafka 解包完成: $KAFKA_HOME"
}

cmd_install() {
    command -v java >/dev/null || die "未找到 java（Kafka 需要 JRE 17+）"
    install_redis
    install_postgres
    install_kafka
    echo
    log "三件套安装完成，下一步: bash scripts/infra.sh start"
}


# --- Redis（分布式限流）------------------------------------------------------

start_redis() {
    [ -x "$REDIS_BIN" ] || die "未找到 Redis，请先 bash scripts/infra.sh install"
    if is_running "$REDIS_PID"; then
        warn "Redis 已在运行 (pid=$(cat "$REDIS_PID"))"
        return 0
    fi
    log "启动 Redis :$REDIS_PORT"
    # liblzf 是解包出来的唯一动态依赖，用 LD_LIBRARY_PATH 指过去
    LD_LIBRARY_PATH="$REDIS_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        "$REDIS_BIN" --port "$REDIS_PORT" --bind 127.0.0.1 \
        --dir "$INFRA_HOME/redis" --save '' --appendonly no \
        > "$LOG_DIR/redis.log" 2>&1 &
    echo $! > "$REDIS_PID"
    if wait_port "$REDIS_PORT" 15; then
        log "Redis 就绪: redis://127.0.0.1:$REDIS_PORT"
    else
        warn "Redis 启动超时，见 $LOG_DIR/redis.log"
    fi
}

stop_redis() { stop_one "$REDIS_PID" "Redis"; }

redis_cli() { LD_LIBRARY_PATH="$REDIS_LIB" "$REDIS_CLI" -p "$REDIS_PORT" "$@"; }


# --- PostgreSQL（请求历史）----------------------------------------------------

# libpq 来自 libpq5 deb，同样解包在 $PG_ROOT 下，PG 的客户端工具需要显式指路
pg_env() { export LD_LIBRARY_PATH="$PG_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"; }

psql_() { pg_env; "$PGBIN/psql" -h 127.0.0.1 -p "$PG_PORT" -U "$PG_USER" "$@"; }

ensure_pg_database() {
    pg_env
    # 幂等：建库 + 给角色设密码（对应 DATABASE_URL 里的 gw:gw）
    if ! psql_ -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$PG_DB'" | grep -q 1; then
        "$PGBIN/createdb" -h 127.0.0.1 -p "$PG_PORT" -U "$PG_USER" -O "$PG_USER" "$PG_DB" \
            || warn "创建数据库 $PG_DB 失败"
    fi
    psql_ -d postgres -c "ALTER ROLE $PG_USER WITH PASSWORD '$PG_PASSWORD'" >/dev/null 2>&1 || true
}

start_postgres() {
    [ -x "$PGBIN/postgres" ] || die "未找到 PostgreSQL，请先 bash scripts/infra.sh install"
    pg_env
    if "$PGBIN/pg_ctl" status -D "$PGDATA" >/dev/null 2>&1; then
        warn "PostgreSQL 已在运行（$PGDATA）"
        return 0
    fi
    if [ ! -s "$PGDATA/PG_VERSION" ]; then
        log "初始化 PostgreSQL 数据目录 $PGDATA（用户 $PG_USER，trust 认证，仅监听 127.0.0.1）"
        mkdir -p "$PGDATA" "$RUN_DIR"
        "$PGBIN/initdb" -D "$PGDATA" -U "$PG_USER" --auth=trust --encoding=UTF8 --locale=C \
            > "$LOG_DIR/pg-initdb.log" 2>&1 || die "initdb 失败，见 $LOG_DIR/pg-initdb.log"
    fi
    log "启动 PostgreSQL :$PG_PORT"
    "$PGBIN/pg_ctl" -D "$PGDATA" -l "$LOG_DIR/postgres.log" \
        -o "-p $PG_PORT -k $RUN_DIR -c listen_addresses=127.0.0.1" start >/dev/null 2>&1 \
        || die "PostgreSQL 启动失败，见 $LOG_DIR/postgres.log"
    wait_port "$PG_PORT" 20 || { warn "PostgreSQL 端口未就绪"; return 1; }
    ensure_pg_database
    log "PostgreSQL 就绪: postgresql://$PG_USER:$PG_PASSWORD@127.0.0.1:$PG_PORT/$PG_DB"
}

stop_postgres() {
    [ -x "$PGBIN/pg_ctl" ] || return 0
    pg_env
    if "$PGBIN/pg_ctl" status -D "$PGDATA" >/dev/null 2>&1; then
        log "停止 PostgreSQL"
        "$PGBIN/pg_ctl" -D "$PGDATA" -m fast stop >/dev/null 2>&1
    else
        warn "PostgreSQL 未在运行"
    fi
}


# --- Kafka（KRaft 单节点：broker + controller 同进程）------------------------

kafka_config() {
    local cfg="$INFRA_HOME/kafka/server.properties" src="$KAFKA_HOME/config/server.properties"
    if [ -s "$cfg" ]; then
        printf '%s' "$cfg"
        return 0
    fi
    [ -f "$src" ] || die "未找到 $src（Kafka 目录结构异常）"
    log "生成 Kafka 配置 $cfg" >&2
    cp -f "$src" "$cfg"
    # Properties 后写的键覆盖先写的，所以直接追加一段覆盖块，不依赖原文件里的注释风格
    cat >> "$cfg" <<EOF

# --- LLM Gateway 本地基础设施覆盖 ---
process.roles=broker,controller
node.id=1
controller.listener.names=CONTROLLER
log.dirs=$KAFKA_DATA
listeners=PLAINTEXT://127.0.0.1:$KAFKA_PORT,CONTROLLER://127.0.0.1:$KAFKA_CONTROLLER_PORT
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://127.0.0.1:$KAFKA_PORT
controller.quorum.voters=1@127.0.0.1:$KAFKA_CONTROLLER_PORT
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
EOF
    printf '%s' "$cfg"
}

create_kafka_topic() {
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:$KAFKA_PORT \
        --create --if-not-exists --topic "$KAFKA_TOPIC" \
        --partitions 3 --replication-factor 1 > "$LOG_DIR/kafka-topic.log" 2>&1 \
        || warn "创建 topic $KAFKA_TOPIC 失败，见 $LOG_DIR/kafka-topic.log"
}

start_kafka() {
    [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ] || die "未找到 Kafka，请先 bash scripts/infra.sh install"
    if is_running "$KAFKA_PID"; then
        warn "Kafka 已在运行 (pid=$(cat "$KAFKA_PID"))"
        return 0
    fi
    local cfg; cfg="$(kafka_config)"
    mkdir -p "$KAFKA_DATA"
    if [ ! -s "$KAFKA_DATA/meta.properties" ]; then
        log "格式化 Kafka 存储目录 $KAFKA_DATA"
        local uuid; uuid="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
        "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$uuid" -c "$cfg" \
            > "$LOG_DIR/kafka-format.log" 2>&1 || die "kafka-storage format 失败，见 $LOG_DIR/kafka-format.log"
    fi
    log "启动 Kafka :$KAFKA_PORT（KRaft 单节点）"
    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$cfg" > "$LOG_DIR/kafka.log" 2>&1 &
    echo $! > "$KAFKA_PID"
    if wait_port "$KAFKA_PORT" 90; then
        log "Kafka 就绪: localhost:$KAFKA_PORT"
        create_kafka_topic
    else
        warn "Kafka 启动超时，见 $LOG_DIR/kafka.log"
    fi
}

stop_kafka() { stop_one "$KAFKA_PID" "Kafka"; }


# --- 命令 -------------------------------------------------------------------

cmd_start() {
    start_redis
    start_postgres
    start_kafka
    echo
    cmd_status
    echo
    log "把网关切到这套基础设施（示例）:"
    cat <<'EOF'
  REDIS_URL=redis://localhost:6379/0 \
  DATABASE_URL=postgresql://gw:gw@localhost:5433/gateway \
  KAFKA_BOOTSTRAP=localhost:9092 \
  python main.py
EOF
}

cmd_stop() {
    stop_kafka
    stop_postgres
    stop_redis
}

cmd_status() {
    echo "--- 基础设施状态 ---"
    if is_running "$REDIS_PID"; then
        printf 'Redis      : pid=%s  :%s  ' "$(cat "$REDIS_PID")" "$REDIS_PORT"
        redis_cli ping 2>/dev/null || echo "(无响应)"
    else
        echo "Redis      : 未运行"
    fi
    pg_env
    if [ -x "$PGBIN/pg_ctl" ] && "$PGBIN/pg_ctl" status -D "$PGDATA" >/dev/null 2>&1; then
        echo "PostgreSQL : 运行中  :$PG_PORT  db=$PG_DB"
        psql_ -d "$PG_DB" -tAc "SELECT 'rows=' || count(*) FROM request_history" 2>/dev/null \
            || echo "             (表尚未创建，首次请求后自动建表)"
    else
        echo "PostgreSQL : 未运行"
    fi
    if is_running "$KAFKA_PID"; then
        echo "Kafka      : pid=$(cat "$KAFKA_PID")  :$KAFKA_PORT"
        "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:$KAFKA_PORT \
            --describe --topic "$KAFKA_TOPIC" 2>/dev/null | head -3 \
            || echo "             (topic $KAFKA_TOPIC 尚未创建)"
    else
        echo "Kafka      : 未运行"
    fi
    echo "--- 网关 ---"
    curl -s -m 3 "http://localhost:8000/infra" && echo || echo "  (网关未运行?)"
}

cmd_logs() {
    for f in redis postgres kafka; do
        echo "===== $f.log（末尾 20 行）====="
        tail -n 20 "$LOG_DIR/$f.log" 2>/dev/null || warn "无 $LOG_DIR/$f.log"
    done
}

case "${1:-start}" in
    install) cmd_install ;;
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_stop; sleep 1; cmd_start ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    *)       die "未知命令: $1（可用: install|start|stop|restart|status|logs）" ;;
esac


