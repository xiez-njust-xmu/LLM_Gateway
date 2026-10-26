"""分布式限流：Redis 固定窗口计数（多实例共享额度）+ 进程内内存回退。

为什么是「Redis + Lua」：

- 多实例部署时每实例各记一份计数，额度就变成 `实例数 × 配额`；把计数放在 Redis
  里由所有实例共享，`/v1/chat/completions` 在任何一台机器上都会看到同一份用量。
- `INCR` 和 `EXPIRE` 必须原子，否则在两步之间崩溃会留下永不过期的 key。这里用一段
  Lua 脚本让 Redis 单线程一次执行完（redis-py 会自动用 EVALSHA 复用脚本缓存）。

Redis 不可用时（没装 / 连接失败 / 运行中抖动）的降级策略由 `REDIS_FAIL_OPEN` 决定：

- `fail-open`（默认）：本实例暂时用进程内计数兜底，请求照常放行，保住可用性；
- `fail-closed`：直接拒绝（429），适合必须严守配额的场景。

降级后后台每 `RECONNECT_INTERVAL` 秒重试一次连接，恢复后自动切回 Redis 共享计数。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from prometheus_metrics import (
    RATE_LIMIT_BACKEND_INFO,
    RATE_LIMIT_BACKEND_UP,
    RATE_LIMIT_REDIS_ERRORS_TOTAL,
)

log = logging.getLogger("gateway.ratelimit")

#: Redis 断线后的重连间隔（秒）
RECONNECT_INTERVAL = 5.0

#: 固定窗口：key 不存在时由本次 INCR 初始化为 1 并设置窗口过期时间；
#: 返回 [当前窗口计数, key 剩余存活毫秒]
_FIXED_WINDOW_LUA = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return {count, redis.call('PTTL', KEYS[1])}
"""


@dataclass
class RateLimitResult:
    """一次限流判定的结果（同时用于生成响应头与事件）。"""

    allowed: bool
    limit: int
    remaining: int
    reset_after: float
    backend: str          # redis / memory / redis-down
    reason: str = ""      # 拒绝或降级原因，写进日志与事件

    def headers(self) -> dict:
        """标准限流响应头（draft-ietf-httpapi-ratelimit-headers 风格）。"""
        headers = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": str(max(1, int(round(self.reset_after)))),
        }
        if not self.allowed:
            headers["Retry-After"] = str(max(1, int(round(self.reset_after))))
        return headers


class MemoryRateLimiter:
    """进程内固定窗口计数：单实例语义，也是 Redis 不可用时的兜底实现。"""

    backend = "memory"

    def __init__(self, window: float):
        self.window = window
        self._buckets: dict[str, tuple[float, int]] = {}

    def check(self, api_key: str, limit: int, tier: str) -> RateLimitResult:
        now = time.time()
        window_start, count = self._buckets.get(api_key, (now, 0))
        if now - window_start >= self.window:
            window_start, count = now, 0
        count += 1
        self._buckets[api_key] = (window_start, count)
        reset_after = max(0.0, self.window - (now - window_start))
        return RateLimitResult(
            allowed=count <= limit,
            limit=limit,
            remaining=max(0, limit - count),
            reset_after=reset_after,
            backend=self.backend,
        )


class RedisRateLimiter:
    """基于 Redis 的固定窗口计数：多实例共享同一份额度。"""

    backend = "redis"

    def __init__(self, client, window: float, prefix: str = "gw:rl"):
        self._client = client
        self.window = window
        self.prefix = prefix

    async def check(self, api_key: str, limit: int, tier: str) -> RateLimitResult:
        now = time.time()
        # 窗口 id 拼进 key：窗口一到自然换 key，无需额外的清理任务
        key = f"{self.prefix}:{tier}:{api_key}:{int(now // self.window)}"
        count, ttl = await self._client.eval(
            _FIXED_WINDOW_LUA, 1, key, int(self.window * 1000)
        )
        count = int(count)
        ttl_ms = int(ttl or -1)
        reset_after = ttl_ms / 1000 if ttl_ms > 0 else float(self.window)
        return RateLimitResult(
            allowed=count <= limit,
            limit=limit,
            remaining=max(0, limit - count),
            reset_after=reset_after,
            backend=self.backend,
        )


class RateLimiter:
    """限流门面：对 main.py 只暴露 `start / check / close / status`。

    `backend="auto"`（默认）时能连上 Redis 就用 Redis；连不上就用内存后端，
    不会因为基础设施缺失把整个网关拖挂。`backend="redis"` 时连不上则按
    `fail_open` 决定是放行（内存兜底）还是拒绝（fail-closed）。
    """

    def __init__(self, url: str, window: int, backend: str = "auto",
                 timeout: float = 1.0, fail_open: bool = True):
        self.url = url
        self.window = window
        self.mode = backend
        self.timeout = timeout
        self.fail_open = fail_open

        self._client = None
        self._redis: RedisRateLimiter | None = None
        self._memory = MemoryRateLimiter(window)
        self._reconnect_task: asyncio.Task | None = None
        self.redis_up = False
        self.redis_error = ""

    # --- 生命周期 -----------------------------------------------------------

    async def start(self) -> None:
        if self.mode == "memory":
            log.info("限流后端: memory（显式指定，单实例计数）")
            self._publish_backend()
            return

        if not await self._connect() and self.mode == "redis":
            log.warning("限流后端: Redis 不可用（%s），%s", self.redis_error,
                        "fail-open → 内存兜底" if self.fail_open else "fail-closed → 拒绝请求")
        self._publish_backend()

    async def close(self) -> None:
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            except Exception:                              # noqa: BLE001
                pass
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:                              # noqa: BLE001
                pass

    async def _connect(self) -> bool:
        """建立 Redis 连接并 ping 一次；失败则登记状态并安排重连。"""
        try:
            import redis.asyncio as aioredis
        except ImportError:
            self.redis_error = "未安装 redis 包（pip install redis）"
            RATE_LIMIT_BACKEND_UP.set(0)
            return False

        try:
            if self._client is None:
                self._client = aioredis.from_url(
                    self.url,
                    socket_timeout=self.timeout,
                    socket_connect_timeout=self.timeout,
                    health_check_interval=30,
                    decode_responses=False,
                )
            await asyncio.wait_for(self._client.ping(), timeout=self.timeout)
        except Exception as exc:                           # noqa: BLE001 连接问题一律降级
            self.redis_error = f"{type(exc).__name__}: {exc}"
            self.redis_up = False
            RATE_LIMIT_BACKEND_UP.set(0)
            self._ensure_reconnect()
            return False

        self._redis = RedisRateLimiter(self._client, self.window)
        self.redis_up = True
        self.redis_error = ""
        RATE_LIMIT_BACKEND_UP.set(1)
        log.info("限流后端: Redis（%s，窗口 %ss，多实例共享计数）", self.url, self.window)
        self._publish_backend()
        return True

    def _ensure_reconnect(self) -> None:
        """Redis 掉线后启动后台重连任务（幂等）。"""
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        while not self.redis_up:
            await asyncio.sleep(RECONNECT_INTERVAL)
            try:
                if await self._connect():
                    log.info("限流后端: Redis 已恢复，切回共享计数")
                    return
            except asyncio.CancelledError:
                raise
            except Exception:                              # noqa: BLE001
                continue

    # --- 判定 ---------------------------------------------------------------

    async def check(self, api_key: str, tier: str, limit: int) -> RateLimitResult:
        """返回限流判定；Redis 异常时按 `fail_open` 决定降级行为。"""
        if self.mode == "memory":
            return self._memory.check(api_key, limit, tier)

        if self.redis_up and self._redis is not None:
            try:
                return await self._redis.check(api_key, limit, tier)
            except Exception as exc:                       # noqa: BLE001
                RATE_LIMIT_REDIS_ERRORS_TOTAL.inc()
                self.redis_up = False
                self.redis_error = f"{type(exc).__name__}: {exc}"
                RATE_LIMIT_BACKEND_UP.set(0)
                self._publish_backend()
                log.warning("Redis 限流失败，降级: %s", self.redis_error)
                self._ensure_reconnect()

        # 降级路径
        if self.fail_open:
            result = self._memory.check(api_key, limit, tier)
            result.reason = f"redis unavailable: {self.redis_error}"
            return result

        return RateLimitResult(
            allowed=False, limit=limit, remaining=0,
            reset_after=float(self.window), backend="redis-down",
            reason=f"redis unavailable (fail-closed): {self.redis_error}",
        )

    # --- 观测 ---------------------------------------------------------------

    def active_backend(self) -> str:
        if self.mode == "memory":
            return "memory"
        if self.redis_up:
            return "redis"
        return "memory" if self.fail_open else "redis-down"

    def _publish_backend(self) -> None:
        """把「当前生效的后端」暴露成 gauge，Grafana 一眼看出是否降级。"""
        active = self.active_backend()
        for name in ("redis", "memory", "redis-down"):
            RATE_LIMIT_BACKEND_INFO.labels(backend=name).set(1 if name == active else 0)

    def status(self) -> dict:
        return {
            "mode": self.mode,
            "active": self.active_backend(),
            "redis_up": self.redis_up,
            "redis_url": self.url,
            "window_seconds": self.window,
            "fail_open": self.fail_open,
            "error": self.redis_error,
        }
