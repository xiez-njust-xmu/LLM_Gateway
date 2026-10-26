#!/usr/bin/env bash
# 安装 Prometheus + Grafana（原生二进制，无需 Docker）
#
# 用法:
#   bash scripts/install_monitoring.sh
#   MON_HOME=/data/monitoring bash scripts/install_monitoring.sh
#
# 说明:
#   本机没有 Docker/podman，因此直接下载官方 release 压缩包解压到 $MON_HOME。
#   重复执行会跳过已下载/已解压的内容（幂等）。
set -euo pipefail

MON_HOME="${MON_HOME:-$HOME/opt/monitoring}"
PROM_VERSION="${PROM_VERSION:-3.15.0}"
GRAFANA_VERSION="${GRAFANA_VERSION:-13.2.3}"

PROM_TARBALL="prometheus-${PROM_VERSION}.linux-amd64.tar.gz"
PROM_URL="https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${PROM_TARBALL}"
GRAFANA_TARBALL="grafana-${GRAFANA_VERSION}.linux-amd64.tar.gz"
GRAFANA_URL="https://dl.grafana.com/oss/release/${GRAFANA_TARBALL}"

DOWNLOADS="$MON_HOME/downloads"
mkdir -p "$DOWNLOADS"

log() { printf '\033[1;34m[install]\033[0m %s\n' "$*"; }

download() {
    local url="$1" dest="$2"
    if [ -s "$dest" ]; then
        log "已存在，跳过下载: $(basename "$dest")"
        return 0
    fi
    log "下载: $url"
    curl -fL --retry 3 --retry-delay 2 --connect-timeout 15 \
        -o "$dest.part" "$url"
    mv "$dest.part" "$dest"
}

# 解压 tarball 并把唯一的顶层目录移动为 $2
install_tarball() {
    local tarball="$1" target="$2"
    local tmp inner
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN
    tar -xzf "$tarball" -C "$tmp"
    inner="$(find "$tmp" -maxdepth 1 -mindepth 1 -type d | head -n 1)"
    if [ -z "$inner" ]; then
        echo "解压后未找到顶层目录: $tarball" >&2
        return 1
    fi
    rm -rf "$target"
    mv "$inner" "$target"
}

# --- Prometheus -------------------------------------------------------------
if [ -x "$MON_HOME/prometheus/prometheus" ]; then
    log "Prometheus 已安装: $MON_HOME/prometheus/prometheus"
else
    download "$PROM_URL" "$DOWNLOADS/$PROM_TARBALL"
    log "解压 Prometheus -> $MON_HOME/prometheus"
    install_tarball "$DOWNLOADS/$PROM_TARBALL" "$MON_HOME/prometheus"
fi

# --- Grafana ----------------------------------------------------------------
if [ -x "$MON_HOME/grafana/bin/grafana" ] || [ -x "$MON_HOME/grafana/bin/grafana-server" ]; then
    log "Grafana 已安装: $MON_HOME/grafana"
else
    download "$GRAFANA_URL" "$DOWNLOADS/$GRAFANA_TARBALL"
    log "解压 Grafana -> $MON_HOME/grafana"
    install_tarball "$DOWNLOADS/$GRAFANA_TARBALL" "$MON_HOME/grafana"
fi

log "准备运行目录"
mkdir -p "$MON_HOME/prometheus/data" \
         "$MON_HOME/grafana/data" \
         "$MON_HOME/grafana/logs" \
         "$MON_HOME/grafana/plugins" \
         "$MON_HOME/run" \
         "$MON_HOME/logs"

echo
log "安装完成，版本信息："
"$MON_HOME/prometheus/prometheus" --version | head -n 2 || true
if [ -x "$MON_HOME/grafana/bin/grafana" ]; then
    "$MON_HOME/grafana/bin/grafana" --version || true
else
    "$MON_HOME/grafana/bin/grafana-server" -v || true
fi
echo
log "接下来运行: bash scripts/monitoring.sh start"