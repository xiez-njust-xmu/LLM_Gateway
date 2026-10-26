import os

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")

# 服务监听地址（供 `python main.py` 直接启动使用）
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8000"))

# 请求未指定 model 时使用的默认别名，必须是 MODEL_ROUTES 中的 key
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "qwen0.5b")

# GPU 指标后台采样间隔（秒）
GPU_SAMPLE_INTERVAL = float(os.getenv("GPU_SAMPLE_INTERVAL", "5"))

# API Keys：生产环境放数据库/环境变量，这里演示
API_KEYS = {
    "sk-test-001": "free",
    "sk-prod-001": "pro",
}

# 限流：每 key 每分钟最大请求数
RATE_LIMIT = {
    "free": 10,
    "pro": 60,
}

# 模型路由表：别名 → Ollama 真实模型名
MODEL_ROUTES = {
    "qwen0.5b": "qwen2.5:0.5b",
    "qwen1.5b": "qwen2.5:1.5b",
    "llama1b": "llama3.2:1b",
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 实例标识：多实例部署时用于区分（写进事件与日志）
GATEWAY_INSTANCE = os.getenv("GATEWAY_INSTANCE",
                             f"gateway-{SERVER_PORT}")

# --- 分布式限流（Redis，多实例共享额度）-------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# auto  : 能连上 Redis 就用 Redis，否则回退进程内计数（默认，最稳）
# redis : 强制 Redis，连不上时按 REDIS_FAIL_OPEN 决定放行还是拒绝
# memory: 强制进程内计数（单实例，便于对照测试）
RATE_LIMIT_BACKEND = os.getenv("RATE_LIMIT_BACKEND", "auto")

# 限流窗口（秒）；窗口内计数超过 RATE_LIMIT[tier] 即 429
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

# Redis 单次操作超时（秒）：限流在请求主链路上，不能等太久
REDIS_TIMEOUT = float(os.getenv("REDIS_TIMEOUT", "1.0"))

# Redis 不可用时是否放行：1=fail-open（默认，可用性优先），0=fail-closed（配额优先）
REDIS_FAIL_OPEN = os.getenv("REDIS_FAIL_OPEN", "1").lower() not in ("0", "false", "no")

# --- 请求历史（SQLite / PostgreSQL）------------------------------------------
# sqlite:///绝对路径            例：sqlite:///./logs/gateway.db（默认）
# postgresql://user:pwd@host:5432/dbname   例：postgresql://gw:gw@localhost:5433/gateway
DATABASE_URL = os.getenv("DATABASE_URL",
                         f"sqlite:///{os.path.join(BASE_DIR, 'logs', 'gateway.db')}")

# 历史写入为异步批量：攒够 HISTORY_BATCH_SIZE 行或等待 HISTORY_FLUSH_INTERVAL 秒后落库
HISTORY_BATCH_SIZE = int(os.getenv("HISTORY_BATCH_SIZE", "20"))
HISTORY_FLUSH_INTERVAL = float(os.getenv("HISTORY_FLUSH_INTERVAL", "1.0"))
HISTORY_QUEUE_SIZE = int(os.getenv("HISTORY_QUEUE_SIZE", "5000"))

# --- 异步事件（Kafka）--------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "llm-gateway-events")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "llm-gateway")
# 1=启用事件发布；0=完全关闭（事件既不进 Kafka 也不写降级文件）
KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "1").lower() not in ("0", "false", "no")
# broker 不可用时事件降级写本地 JSONL，保证主链路与事件都不丢
EVENT_FALLBACK_FILE = os.getenv(
    "EVENT_FALLBACK_FILE", os.path.join(BASE_DIR, "logs", "events.jsonl"))