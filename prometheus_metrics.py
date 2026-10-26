"""Prometheus 指标定义与记录辅助函数。

所有指标注册在 prometheus_client 的默认 REGISTRY 上，由 main.py 的 /metrics
端点暴露。默认注册表还会自动带上 process_* / python_* / gc 等运行时指标。

命名遵循 Prometheus 规范：<namespace>_<subject>_<unit>，计数器以 _total 结尾。
"""

from prometheus_client import Counter, Gauge, Histogram

# --- 请求维度 ---------------------------------------------------------------

REQUESTS_TOTAL = Counter(
    "gateway_requests_total",
    "Gateway 处理的请求总数",
    ["model", "tier", "status", "stream"],
)

REQUEST_DURATION = Histogram(
    "gateway_request_duration_seconds",
    "Gateway 端到端请求耗时（秒）",
    ["model", "tier"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

INFLIGHT = Gauge(
    "gateway_inflight_requests",
    "当前正在处理中的请求数",
)

AUTH_FAILURES_TOTAL = Counter(
    "gateway_auth_failures_total",
    "鉴权失败（无效 API Key）的请求数",
)

RATE_LIMITED_TOTAL = Counter(
    "gateway_rate_limited_total",
    "因超出限流被拒绝的请求数",
    ["tier"],
)

UPSTREAM_ERRORS_TOTAL = Counter(
    "gateway_upstream_errors_total",
    "调用 Ollama 失败（非 2xx 或网络错误）的次数",
    ["model"],
)

# --- 推理质量维度 -----------------------------------------------------------

# Ollama 运行参数标签（参数调优实验按这些维度分组画曲线）；请求未指定时取 "default"
CONFIG_LABELS = ("num_ctx", "num_gpu", "num_batch")
DEFAULT_CONFIG = {key: "default" for key in CONFIG_LABELS}

TTFT = Histogram(
    "gateway_ttft_seconds",
    "首 token 延迟（秒，来自 Ollama load + prompt_eval 耗时）",
    ["model", *CONFIG_LABELS],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

TPS = Histogram(
    "gateway_tokens_per_second",
    "生成速度（tokens/秒）",
    ["model", *CONFIG_LABELS],
    buckets=(1, 5, 10, 20, 40, 60, 80, 100, 150, 200, 300, 500),
)

TOKENS_TOTAL = Counter(
    "gateway_tokens_total",
    "累计 token 数",
    ["model", "tier", "kind"],  # kind: prompt / completion
)

# --- 硬件维度 ---------------------------------------------------------------

GPU_UTILIZATION = Gauge(
    "gateway_gpu_utilization_percent",
    "GPU 利用率（%）",
)

GPU_MEMORY_USED = Gauge(
    "gateway_gpu_memory_used_megabytes",
    "GPU 显存占用（MiB）",
)

# 最近一次采样的 GPU 数值，供日志复用，避免每个请求都调用 nvidia-smi
_last_gpu = (0, 0)


def record_request(model: str, tier: str, status: int, stream: bool = False,
                   duration: float | None = None):
    """记录一次请求的结果（计数 + 可选耗时）。"""
    REQUESTS_TOTAL.labels(
        model=model,
        tier=tier,
        status=str(status),
        stream="true" if stream else "false",
    ).inc()
    if duration is not None:
        REQUEST_DURATION.labels(model=model, tier=tier).observe(duration)


def record_usage(model: str, tier: str, metrics: dict, config: dict | None = None):
    """记录 token 用量、首 token 延迟与生成速度。

    config 为 Ollama 运行参数标签（见 CONFIG_LABELS），用于按 num_ctx /
    num_gpu / num_batch 分组观测；缺省时三个标签均记为 "default"。
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    prompt = metrics.get("prompt_tokens") or 0
    completion = metrics.get("completion_tokens") or 0
    TOKENS_TOTAL.labels(model=model, tier=tier, kind="prompt").inc(prompt)
    TOKENS_TOTAL.labels(model=model, tier=tier, kind="completion").inc(completion)

    ttft = metrics.get("ttft")
    if ttft is not None:
        TTFT.labels(model=model, **cfg).observe(ttft)

    tps = metrics.get("tps")
    if tps:
        TPS.labels(model=model, **cfg).observe(tps)


def record_gpu(util: int, mem: int):
    """更新 GPU 指标并缓存最近一次采样值。"""
    global _last_gpu
    _last_gpu = (util, mem)
    GPU_UTILIZATION.set(util)
    GPU_MEMORY_USED.set(mem)


def current_gpu() -> tuple[int, int]:
    """返回最近一次采样的 GPU（利用率 %, 显存 MiB）。"""
    return _last_gpu


# --- 分布式基础设施维度（限流后端 / 请求历史 / 事件总线）---------------------

RATE_LIMIT_BACKEND_UP = Gauge(
    "gateway_ratelimit_backend_up",
    "Redis 限流后端是否可用（1=共享计数生效，0=已降级为进程内计数）",
)

RATE_LIMIT_BACKEND_INFO = Gauge(
    "gateway_ratelimit_backend_info",
    "当前生效的限流后端（取值为 1 的即为当前后端，其余为 0）",
    ["backend"],  # redis / memory / redis-down
)

RATE_LIMIT_REDIS_ERRORS_TOTAL = Counter(
    "gateway_ratelimit_redis_errors_total",
    "Redis 限流操作失败（连接断开 / 超时）并触发降级的次数",
)

HISTORY_UP = Gauge(
    "gateway_history_up",
    "请求历史存储是否可用（1=可写，0=故障）",
    ["backend"],  # sqlite / postgres
)

HISTORY_WRITES_TOTAL = Counter(
    "gateway_history_writes_total",
    "成功写入请求历史的总行数",
    ["backend"],
)

HISTORY_WRITE_ERRORS_TOTAL = Counter(
    "gateway_history_write_errors_total",
    "请求历史写库失败次数（每批计一次）",
    ["backend"],
)

HISTORY_DROPPED_TOTAL = Counter(
    "gateway_history_dropped_total",
    "因写入队列已满被丢弃的历史记录数（宁可丢历史也不阻塞请求）",
)

HISTORY_ROWS = Gauge(
    "gateway_history_rows",
    "请求历史表行数（本实例统计，stats 查询时刷新为全表真实值）",
    ["backend"],
)

EVENTS_PUBLISHED_TOTAL = Counter(
    "gateway_events_published_total",
    "成功发送到 Kafka 的事件数",
    ["event"],
)

EVENTS_FAILED_TOTAL = Counter(
    "gateway_events_failed_total",
    "发送 Kafka 失败的事件数",
    ["event"],
)

EVENTS_FALLBACK_TOTAL = Counter(
    "gateway_events_fallback_total",
    "Kafka 不可用时降级写入本地 JSONL 的事件数",
)

EVENTS_DROPPED_TOTAL = Counter(
    "gateway_events_dropped_total",
    "因事件队列已满被丢弃的事件数",
)

KAFKA_UP = Gauge(
    "gateway_kafka_up",
    "Kafka 生产者连接状态（1=已连接，0=未连接/降级中）",
)