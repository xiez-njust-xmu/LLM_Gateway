"""异步事件总线：把请求生命周期事件发到 Kafka（网关的「事后分析」通道）。

事件类型（同一个 topic 内用 `event` 字段区分）：

- `request.completed`    成功的请求（含 ttft / tps / token 用量 / 参数标签 / 限流后端）
- `request.rate_limited` 被限流拒绝
- `request.auth_failed`  鉴权失败
- `request.error`        上游 Ollama 错误或请求体非法

为什么用 Kafka 而不是「顺手多写一个日志文件」：

- 同一份事件可以被多个下游各自消费（数仓离线分析、实时告警、BI 看板），互不干扰；
- 网关只负责 produce，消费端慢或挂掉都不会反压推理主链路；
- 事件以 `api_key` 作为分区 key，同一个 key 的事件落进同一分区、天然有序。

可靠性设计：

- `publish()` 只做一次非阻塞入队，后台任务攒批发送（`send_and_wait` 保证送达语义）；
- broker 不可用（没启动 / 网络抖动 / 发送失败）时，事件降级写本地 JSONL
  （`logs/events.jsonl`，带 `fallback_reason`），并每 `RECONNECT_INTERVAL` 秒重连，
  恢复后自动继续发 Kafka；
- 队列满则丢弃并计数（`gateway_events_dropped_total`）：宁可丢事件，也不拖慢请求。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time

from config import (
    EVENT_FALLBACK_FILE,
    GATEWAY_INSTANCE,
    KAFKA_BOOTSTRAP,
    KAFKA_CLIENT_ID,
    KAFKA_ENABLED,
    KAFKA_TOPIC,
)
from prometheus_metrics import (
    EVENTS_DROPPED_TOTAL,
    EVENTS_FAILED_TOTAL,
    EVENTS_FALLBACK_TOTAL,
    EVENTS_PUBLISHED_TOTAL,
    KAFKA_UP,
)

log = logging.getLogger("gateway.events")

#: broker 掉线后的重连间隔（秒）
RECONNECT_INTERVAL = 10.0

#: 单次批量最多发送的事件数
BATCH_SIZE = 100

#: 没有新事件时后台任务的等待时间（秒）
FLUSH_INTERVAL = 0.5

#: 网关会产生的事件类型（main.py 按这些名字发布，同时用于预初始化指标序列）
EVENT_TYPES = ("request.completed", "request.rate_limited",
               "request.auth_failed", "request.error")


class EventBus:
    """Kafka 事件生产者（带本地文件降级），对 main.py 只暴露 publish/status。"""

    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP, topic: str = KAFKA_TOPIC,
                 client_id: str = KAFKA_CLIENT_ID, enabled: bool = KAFKA_ENABLED,
                 fallback_path: str = EVENT_FALLBACK_FILE, queue_size: int = 10000,
                 instance: str = GATEWAY_INSTANCE, start_timeout: float = 5.0):
        self.bootstrap = bootstrap
        self.topic = topic
        self.client_id = client_id
        self.enabled = enabled
        self.fallback_path = fallback_path
        self.instance = instance
        self.start_timeout = start_timeout

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._producer = None
        self._worker: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None

        self.connected = False
        self.error = ""
        self.published = 0
        self.failed = 0
        self.fallback = 0
        self.dropped = 0

    # --- 生命周期 -----------------------------------------------------------

    async def start(self) -> None:
        if not self.enabled:
            KAFKA_UP.set(0)
            log.info("事件总线: 已禁用（KAFKA_ENABLED=0）")
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.fallback_path)), exist_ok=True)
        # 带标签的计数器先落一条 0 值序列，Grafana 面板才不会显示 "No data"
        for event_type in EVENT_TYPES:
            EVENTS_PUBLISHED_TOTAL.labels(event=event_type).inc(0)
            EVENTS_FAILED_TOTAL.labels(event=event_type).inc(0)
        self._worker = asyncio.create_task(self._worker_loop())
        await self._connect()

    async def _connect(self) -> bool:
        """建立 Kafka 生产者；失败不抛错，交给降级与后台重连。"""
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError:
            self.error = "未安装 aiokafka（pip install aiokafka）"
            KAFKA_UP.set(0)
            return False

        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap,
                client_id=self.client_id,
                acks=1,
                linger_ms=20,
                compression_type="gzip",
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                key_serializer=lambda k: str(k).encode() if k else None,
            )
            await asyncio.wait_for(producer.start(), timeout=self.start_timeout)
        except Exception as exc:                           # noqa: BLE001
            self.error = f"{type(exc).__name__}: {exc}"
            self.connected = False
            KAFKA_UP.set(0)
            self._ensure_reconnect()
            return False

        self._producer = producer
        self.connected = True
        self.error = ""
        KAFKA_UP.set(1)
        log.info("事件总线: Kafka %s -> topic %s", self.bootstrap, self.topic)
        return True

    async def close(self) -> None:
        for task in (self._worker, self._reconnect_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:                          # noqa: BLE001
                    pass

        # 退出前排空队列，别丢最后一批事件
        pending = []
        while True:
            try:
                pending.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if pending:
            if self.connected and self._producer is not None:
                await self._send(pending)
            else:
                await self._write_fallback(pending, "gateway shutdown, kafka unavailable")
        await self._reset_producer()

    # --- 发送 ---------------------------------------------------------------

    def publish(self, event_type: str, payload: dict) -> None:
        """发布一个事件（非阻塞；队列满则丢弃并计数）。"""
        if not self.enabled:
            return
        envelope = {
            "event": event_type,
            "ts": round(time.time(), 3),
            "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "instance": self.instance,
            "host": socket.gethostname(),
            "data": payload,
        }
        try:
            self._queue.put_nowait((event_type, envelope))
        except asyncio.QueueFull:
            self.dropped += 1
            EVENTS_DROPPED_TOTAL.inc()

    async def _worker_loop(self) -> None:
        """攒批：等到第一条事件后尽量把队列里已有的都打包成一批发出去。"""
        while True:
            try:
                first = await asyncio.wait_for(self._queue.get(), FLUSH_INTERVAL)
            except asyncio.TimeoutError:
                continue
            batch = [first]
            while len(batch) < BATCH_SIZE:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await self._send(batch)

    async def _send(self, batch: list[tuple]) -> None:
        """发送一批事件；失败则降级写本地文件并触发重连。"""
        if not self.connected or self._producer is None:
            await self._write_fallback(batch, self.error or "kafka not connected")
            return
        try:
            for _, envelope in batch:
                key = (envelope.get("data") or {}).get("api_key")
                await self._producer.send_and_wait(self.topic, value=envelope, key=key)
        except Exception as exc:                           # noqa: BLE001
            self.error = f"{type(exc).__name__}: {exc}"
            self.connected = False
            KAFKA_UP.set(0)
            self.failed += len(batch)
            for event_type, _ in batch:
                EVENTS_FAILED_TOTAL.labels(event=event_type).inc()
            log.warning("Kafka 发送失败，降级写本地文件: %s", self.error)
            await self._write_fallback(batch, self.error)
            await self._reset_producer()
            self._ensure_reconnect()
            return

        self.published += len(batch)
        for event_type, _ in batch:
            EVENTS_PUBLISHED_TOTAL.labels(event=event_type).inc()

    async def _write_fallback(self, batch: list[tuple], reason: str) -> None:
        """broker 不可用时把事件追加到本地 JSONL，保证事件不丢。"""

        def _write() -> None:
            with open(self.fallback_path, "a", encoding="utf-8") as fp:
                for _, envelope in batch:
                    envelope["fallback_reason"] = reason
                    fp.write(json.dumps(envelope, ensure_ascii=False) + "\n")

        try:
            await asyncio.to_thread(_write)
        except Exception as exc:                           # noqa: BLE001
            self.error = f"fallback write failed: {exc}"
            log.error("事件降级落盘也失败: %s", exc)
            return
        self.fallback += len(batch)
        EVENTS_FALLBACK_TOTAL.inc(len(batch))

    async def _reset_producer(self) -> None:
        producer, self._producer = self._producer, None
        if producer is not None:
            try:
                await producer.stop()
            except Exception:                              # noqa: BLE001
                pass

    # --- 重连 ---------------------------------------------------------------

    def _ensure_reconnect(self) -> None:
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        while not self.connected:
            await asyncio.sleep(RECONNECT_INTERVAL)
            try:
                if await self._connect():
                    log.info("事件总线: Kafka 已恢复，继续发送事件")
                    return
            except asyncio.CancelledError:
                raise
            except Exception:                              # noqa: BLE001
                continue

    # --- 观测 ---------------------------------------------------------------

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "connected": self.connected,
            "bootstrap": self.bootstrap,
            "topic": self.topic,
            "queued": self._queue.qsize(),
            "published": self.published,
            "failed": self.failed,
            "fallback_written": self.fallback,
            "dropped": self.dropped,
            "fallback_file": self.fallback_path,
            "error": self.error,
        }
