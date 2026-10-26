#!/usr/bin/env python
"""基础设施验收脚本：分布式限流（Redis）/ 请求历史（SQLite·Postgres）/ Kafka 事件。

脚本会临时拉起两个网关实例（默认 :8000 / :8001），用同一份配置做对照实验：

1. `/health`、`/infra` 状态：确认三个后端都连上了；
2. 发一次非流式 + 一次流式请求，检查历史落库、事件外发；
3. 请求历史：`/history` 明细、`/history/stats` 聚合、直连数据库核对行数与内容；
4. **分布式限流对照**（核心）：
     - redis 模式：两个实例共享同一份额度，总共 12 次请求（各 6 次）只应放行 10 次；
     - memory 模式：两个实例各自计数，12 次请求应全部放行 —— 反证「共享」确实生效；
5. Kafka：消费 topic，统计各类型事件条数（request.completed / request.rate_limited ...）。

用法：
    python verify_infra.py                          # 全量
    python verify_infra.py --skip-kafka             # 不检查 Kafka
    python verify_infra.py --keep-instances         # 跑完不关掉实例（排障用）

依赖运行中的 Ollama；基础设施可用 bash scripts/infra.sh start 拉起。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
FREE_KEY = "sk-test-001"          # free 档：RATE_LIMIT["free"] = 10 次 / 窗口
FREE_LIMIT = 10
CHAT = {"model": "qwen0.5b", "options": {"num_predict": 4, "num_ctx": 512}}

_results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    _results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


class GatewayInstance:
    """一个临时网关进程（用于「多实例」对照）。"""

    def __init__(self, port: int, env: dict):
        self.port = port
        self.env = env
        self.proc: subprocess.Popen | None = None
        self.log = os.path.join(BASE_DIR, "logs", f"verify_gateway_{port}.log")
        self.base = f"http://localhost:{port}"

    def start(self, timeout: float = 40.0) -> bool:
        env = {**os.environ, **self.env,
               "SERVER_PORT": str(self.port),
               "GATEWAY_INSTANCE": f"gateway-{self.port}"}
        os.makedirs(os.path.dirname(self.log), exist_ok=True)
        handle = open(self.log, "w", encoding="utf-8")
        self.proc = subprocess.Popen([PY, "main.py"], cwd=BASE_DIR, env=env,
                                     stdout=handle, stderr=subprocess.STDOUT)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if requests.get(f"{self.base}/health", timeout=2).status_code == 200:
                    return True
            except requests.RequestException:
                pass
            time.sleep(0.5)
        return False

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def chat(self, api_key: str = FREE_KEY, stream: bool = False, **extra) -> requests.Response:
        payload = {**CHAT, "messages": [{"role": "user", "content": "只回复 OK"}],
                   "stream": stream, **extra}
        return requests.post(f"{self.base}/v1/chat/completions", json=payload,
                             headers={"X-API-Key": api_key}, timeout=180)

    def query(self, path: str, **params) -> requests.Response:
        return requests.get(f"{self.base}{path}", params=params, timeout=30)


def reset_rate_limit(redis_url: str) -> None:
    """清掉限流 key，让本次测量从干净的窗口开始。"""
    try:
        import redis
        client = redis.Redis.from_url(redis_url)
        keys = list(client.scan_iter("gw:rl:*"))
        if keys:
            client.delete(*keys)
        client.close()
    except Exception as exc:                               # noqa: BLE001
        print(f"  (清理 Redis 限流 key 失败，忽略: {exc})")


# --- 各项检查 -----------------------------------------------------------------

def check_status(inst: GatewayInstance) -> None:
    print("\n[1] 服务状态")
    health = inst.query("/health").json()
    check("限流后端 = redis（共享计数生效）", health.get("rate_limit_backend") == "redis",
          str(health.get("rate_limit_backend")))
    check("历史后端已启用", health.get("history_backend") in ("sqlite", "postgres"),
          str(health.get("history_backend")))
    check("Kafka 已连接", health.get("kafka_connected") is True,
          str(health.get("kafka_connected")))

    infra = inst.query("/infra").json()
    check("Redis PING 正常", infra["rate_limit"]["redis_up"] is True,
          infra["rate_limit"].get("error", ""))
    check("历史写入队列已清空", infra["history"]["queued"] == 0,
          f"queued={infra['history']['queued']} written={infra['history']['written']}")
    print(f"        instance={infra['instance']}  history={infra['history']['dsn']}"
          f"  topic={infra['events']['topic']}")


def check_requests_and_history(inst: GatewayInstance) -> None:
    print("\n[2] 请求 -> 历史落库 + 事件外发")
    resp = inst.chat(stream=False)
    check("非流式请求 200", resp.status_code == 200, str(resp.status_code))
    check("响应带限流头 X-RateLimit-*", "X-RateLimit-Remaining" in resp.headers,
          f"remaining={resp.headers.get('X-RateLimit-Remaining')}")

    stream_ok = False
    with requests.post(f"{inst.base}/v1/chat/completions",
                       json={**CHAT, "stream": True,
                             "stream_options": {"include_usage": True},
                             "messages": [{"role": "user", "content": "OK"}]},
                       headers={"X-API-Key": FREE_KEY}, stream=True, timeout=180) as r:
        # 必须把整个流读干净：中途断开会让网关侧生成器被取消，拿不到 done 分片的用量
        lines = list(r.iter_lines()) if r.status_code == 200 else []
    stream_ok = bool(lines) and any(b"data:" in line for line in lines)
    check("流式请求 200 且返回完整 SSE", stream_ok, f"{len(lines)} 个分片")

    time.sleep(3)     # 等异步批量写库 + 事件发送
    history = inst.query("/history", limit=10).json()
    items = history.get("items", [])
    check("/history 返回记录", len(items) >= 2,
          f"{len(items)} 条，backend={history.get('backend')}")
    if items:
        check("历史带 Ollama 参数标签", all(items[0].get(k) for k in ("num_ctx", "num_gpu", "num_batch")),
              str({k: items[0].get(k) for k in ("num_ctx", "num_gpu", "num_batch")}))
        check("历史含 TTFT / TPS / 限流后端", bool(items[0].get("ttft") is not None
                                                and items[0].get("tps")),
              f"ttft={items[0].get('ttft')} tps={items[0].get('tps')}"
              f" rl={items[0].get('rate_limit_backend')}")

    stats = inst.query("/history/stats", window=3600).json()
    totals = stats.get("totals", {})
    check("/history/stats 聚合可用", totals.get("requests", 0) >= 2,
          f"requests={totals.get('requests')} avg_ttft={totals.get('avg_ttft')}"
          f" p95_wall={totals.get('p95_wall_time')} avg_tps={totals.get('avg_tps')}")
    print(f"        by_model={json.dumps(stats.get('by_model'), ensure_ascii=False)}")


async def check_database(db_url: str) -> None:
    print("\n[3] 直连数据库核对（复用网关的 storage 层）")
    sys.path.insert(0, BASE_DIR)
    from storage import RequestHistory

    hist = RequestHistory(db_url, flush_interval=0.2)
    await hist.start()
    try:
        status = hist.status()
        if not check("数据库可连接", status["enabled"], status["error"] or status["dsn"]):
            return
        rows = await hist.recent(limit=200)
        check("表中已有历史行", len(rows) > 0,
              f"{len(rows)} 行，backend={status['backend']}")
        if rows:
            fields = ("ts", "model", "status", "stream", "ttft", "tps",
                      "num_ctx", "num_gpu", "num_batch", "rate_limit_backend", "instance")
            print("        " + json.dumps({k: rows[0].get(k) for k in fields}, ensure_ascii=False))
        stats = await hist.stats(window_sec=86400)
        check("库内聚合统计非空", stats["totals"].get("requests", 0) > 0,
              f"rows_total={stats.get('rows_total')}")
    finally:
        await hist.close()


def burst(port: int, count: int = 6) -> tuple[int, int]:
    """对一个实例连发 count 次请求，返回 (放行, 被拒) 计数。"""
    allowed = denied = 0
    for _ in range(count):
        resp = requests.post(f"http://localhost:{port}/v1/chat/completions",
                             json={**CHAT, "messages": [{"role": "user", "content": "OK"}]},
                             headers={"X-API-Key": FREE_KEY}, timeout=180)
        allowed += resp.status_code == 200
        denied += resp.status_code == 429
    return allowed, denied


def check_rate_limit_shared(instances: list[GatewayInstance], redis_url: str) -> None:
    print("\n[4] 分布式限流：两实例共享同一份额度（Redis 后端）")
    reset_rate_limit(redis_url)
    a = burst(instances[0].port)
    b = burst(instances[1].port)
    allowed, denied = a[0] + b[0], a[1] + b[1]
    check(f"12 次请求（各 6 次）只放行 {FREE_LIMIT} 次",
          allowed == FREE_LIMIT and denied == 12 - FREE_LIMIT,
          f"实例A 放行/拒={a}  实例B 放行/拒={b}  -> 合计 放行={allowed} 拒绝={denied}")


def check_rate_limit_memory(instances: list[GatewayInstance]) -> None:
    print("\n[5] 对照：内存后端（各实例独立计数，额度被放大成 2×10）")
    a = burst(instances[0].port)
    b = burst(instances[1].port)
    allowed, denied = a[0] + b[0], a[1] + b[1]
    check("内存后端下 12 次请求全部放行（反证 Redis 才是共享的）",
          allowed == 12 and denied == 0,
          f"实例A 放行/拒={a}  实例B 放行/拒={b}  -> 合计 放行={allowed} 拒绝={denied}")


def check_events(topic: str, bootstrap: str, timeout: float = 8.0) -> None:
    print("\n[6] Kafka 事件消费")

    async def consume():
        from aiokafka import AIOKafkaConsumer
        consumer = AIOKafkaConsumer(
            topic, bootstrap_servers=bootstrap,
            group_id=f"verify-infra-{int(time.time())}",
            auto_offset_reset="earliest", enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode()),
        )
        await consumer.start()
        counts: dict[str, int] = {}
        instances: set[str] = set()
        sample = None
        try:
            deadline = time.time() + timeout
            while time.time() < deadline:
                batches = await consumer.getmany(timeout_ms=1000)
                for messages in batches.values():
                    for msg in messages:
                        event = msg.value
                        name = event.get("event", "?")
                        counts[name] = counts.get(name, 0) + 1
                        instances.add(event.get("instance"))
                        if sample is None and name == "request.completed":
                            sample = event
        finally:
            await consumer.stop()
        return counts, instances, sample

    try:
        counts, instances, sample = asyncio.run(consume())
    except Exception as exc:                               # noqa: BLE001
        check("消费 Kafka 事件", False, f"{type(exc).__name__}: {exc}")
        return
    check("收到 request.completed 事件", counts.get("request.completed", 0) > 0, str(counts))
    check("收到 request.rate_limited 事件", counts.get("request.rate_limited", 0) > 0)
    check("事件带实例标识（多实例可区分）", bool(instances),
          f"instances={sorted(i for i in instances if i)}")
    if sample:
        data = sample.get("data") or {}
        print("        样例事件: " + json.dumps({
            "event": sample.get("event"), "instance": sample.get("instance"),
            "model": data.get("model"), "status": data.get("status"),
            "ttft": data.get("ttft"), "tps": data.get("tps"),
            "num_ctx": data.get("num_ctx"), "rate_limit_backend": data.get("rate_limit_backend"),
        }, ensure_ascii=False))


def main() -> int:
    ap = argparse.ArgumentParser(description="基础设施验收：Redis 限流 / 请求历史 / Kafka 事件")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--database-url", default=os.getenv("DATABASE_URL")
                    or f"sqlite:///{os.path.join(BASE_DIR, 'logs', 'gateway.db')}")
    ap.add_argument("--kafka", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "llm-gateway-events"))
    ap.add_argument("--port", type=int, default=8000, help="第一个实例端口（第二个为其 +1）")
    ap.add_argument("--skip-kafka", action="store_true")
    ap.add_argument("--keep-instances", action="store_true", help="跑完保留实例（排障用）")
    args = ap.parse_args()

    print(f"Redis    = {args.redis_url}")
    print(f"数据库   = {args.database_url}")
    print(f"Kafka    = {args.kafka}  topic={args.topic}")

    common = {"DATABASE_URL": args.database_url, "KAFKA_BOOTSTRAP": args.kafka,
              "KAFKA_TOPIC": args.topic, "RATE_LIMIT_WINDOW": "60"}

    print(f"\n拉起两个网关实例（Redis 共享限流）: :{args.port} / :{args.port + 1}")
    redis_instances = [
        GatewayInstance(args.port, {**common, "REDIS_URL": args.redis_url,
                                    "RATE_LIMIT_BACKEND": "redis"}),
        GatewayInstance(args.port + 1, {**common, "REDIS_URL": args.redis_url,
                                        "RATE_LIMIT_BACKEND": "redis"}),
    ]
    memory_instances = [
        GatewayInstance(args.port, {**common, "RATE_LIMIT_BACKEND": "memory"}),
        GatewayInstance(args.port + 1, {**common, "RATE_LIMIT_BACKEND": "memory"}),
    ]

    try:
        if not all(inst.start() for inst in redis_instances):
            check("两个网关实例启动成功", False,
                  f"见 {redis_instances[0].log} / {redis_instances[1].log}")
            return 1
        check("两个网关实例启动成功", True, f":{args.port} & :{args.port + 1}")

        check_status(redis_instances[0])
        check_requests_and_history(redis_instances[0])
        asyncio.run(check_database(args.database_url))
        check_rate_limit_shared(redis_instances, args.redis_url)
        if not args.skip_kafka:
            check_events(args.topic, args.kafka)

        print(f"\n切换为内存限流后端对照: :{args.port} / :{args.port + 1}")
        for inst in redis_instances:
            inst.stop()
        time.sleep(1)
        if all(inst.start() for inst in memory_instances):
            check_rate_limit_memory(memory_instances)
        else:
            check("内存后端实例启动成功", False)
    finally:
        if not args.keep_instances:
            for inst in redis_instances + memory_instances:
                inst.stop()

    failed = [name for name, ok, _ in _results if not ok]
    print("\n" + "=" * 66)
    print(f"验收结果: {len(_results) - len(failed)}/{len(_results)} 项通过")
    for name in failed:
        print(f"  FAIL  {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())


