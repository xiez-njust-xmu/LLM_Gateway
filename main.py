import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager, suppress

import httpx
from fastapi import FastAPI, Request, Header, HTTPException, Response
from fastapi.responses import StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from config import (
    OLLAMA_URL,
    RATE_LIMIT,
    DEFAULT_MODEL,
    SERVER_HOST,
    SERVER_PORT,
    GPU_SAMPLE_INTERVAL,
    GATEWAY_INSTANCE,
    # 分布式限流（Redis）
    RATE_LIMIT_BACKEND,
    RATE_LIMIT_WINDOW,
    REDIS_URL,
    REDIS_TIMEOUT,
    REDIS_FAIL_OPEN,
    # 请求历史（SQLite / Postgres）
    DATABASE_URL,
    HISTORY_BATCH_SIZE,
    HISTORY_FLUSH_INTERVAL,
    HISTORY_QUEUE_SIZE,
)
from auth import verify_api_key
from ratelimit import RateLimiter
from router import resolve_model
from logger import log_request
from metrics import get_gpu_stats, compute_metrics
from storage import RequestHistory
from events import EventBus
from prometheus_metrics import (
    AUTH_FAILURES_TOTAL,
    CONFIG_LABELS,
    INFLIGHT,
    RATE_LIMITED_TOTAL,
    UPSTREAM_ERRORS_TOTAL,
    current_gpu,
    record_gpu,
    record_request,
    record_usage,
)

# Ollama 请求超时：连接快超时，读取放宽（本地模型可能加载较慢）
REQUEST_TIMEOUT = httpx.Timeout(connect=5.0, read=300.0, write=30.0, pool=5.0)

# 基础设施（限流 / 历史 / 事件）的日志走标准 logging，便于一眼看到降级信息
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)


async def _gpu_sampler():
    """后台定时采样 GPU 指标，保证空闲时曲线也连续。"""
    while True:
        try:
            util, mem = await asyncio.to_thread(get_gpu_stats)
            record_gpu(util, mem)
        except Exception:
            pass
        await asyncio.sleep(GPU_SAMPLE_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_gpu_sampler())
    # 三件基础设施各自独立降级：任何一个连不上都不会影响推理主链路
    await rate_limiter.start()   # Redis 分布式限流（连不上则进程内计数）
    await history.start()        # 请求历史 SQLite / Postgres
    await events.start()         # Kafka 异步事件（连不上则降级本地 JSONL）
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        # 关停顺序：先把积压的事件发完/落盘，再停历史写入，最后断开 Redis
        await events.close()
        await history.close()
        await rate_limiter.close()


app = FastAPI(title="LLM Gateway", version="0.2", lifespan=lifespan)

# --- 基础设施单例 -----------------------------------------------------------
# 限流：Redis 固定窗口，多实例共享额度；不可用时按 REDIS_FAIL_OPEN 降级
rate_limiter = RateLimiter(
    url=REDIS_URL,
    window=RATE_LIMIT_WINDOW,
    backend=RATE_LIMIT_BACKEND,
    timeout=REDIS_TIMEOUT,
    fail_open=REDIS_FAIL_OPEN,
)

# 请求历史：默认 SQLite，把 DATABASE_URL 换成 postgresql://... 即切到 Postgres
history = RequestHistory(
    DATABASE_URL,
    batch_size=HISTORY_BATCH_SIZE,
    flush_interval=HISTORY_FLUSH_INTERVAL,
    queue_size=HISTORY_QUEUE_SIZE,
)

# 异步事件：请求生命周期事件 -> Kafka
events = EventBus()


def _persist(record: dict, event: str = "request.completed") -> None:
    """一条请求记录同时落三处：JSONL 文件日志 + 历史库 + Kafka 事件。

    三处都是「尽力而为」：`history.submit` / `events.publish` 只做入队，
    队列满或后端不可用都只计数不抛错，绝不反压推理主链路。
    """
    log_request(record)
    history.submit(record)
    events.publish(event, record)


def _new_completion_id() -> str:
    return f"chatcmpl-{int(time.time() * 1000)}"


def _stream_chunk(completion_id: str, model: str, content: str, finish_reason=None):
    """构造 OpenAI 兼容的流式分片。"""
    return {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
            "index": 0,
            "delta": {"content": content} if content else {},
            "finish_reason": finish_reason,
        }],
    }


def _sse(payload) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


# Ollama 参数白名单：参数名 -> (允许的类型, 最小值, 最大值)。
# 只放行已知的 runner/生成参数，防止任意字段注入上游；
# num_ctx / num_gpu / num_batch 会额外作为 Prometheus 标签（见 CONFIG_LABELS）。
OPTION_SPECS: dict[str, tuple] = {
    "num_ctx":        ((int,), 64, 131072),
    "num_gpu":        ((int,), -1, 64),
    "num_batch":      ((int,), 1, 8192),
    "num_predict":    ((int,), -1, 4096),
    "num_thread":     ((int,), 1, 256),
    "temperature":    ((int, float), 0.0, 2.0),
    "top_k":          ((int,), 1, 100000),
    "top_p":          ((int, float), 0.0, 1.0),
    "repeat_penalty": ((int, float), 0.0, 2.0),
}


def _parse_options(body: dict) -> tuple[dict, dict]:
    """解析并校验请求体里的 `options`，返回 (透传给 Ollama 的 dict, 指标标签 dict)。

    未提供 options 时返回空 dict 与全为 "default" 的标签，保证指标标签始终存在。
    校验失败抛 400。
    """
    raw = body.get("options")
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail="options must be a JSON object")

    options: dict = {}
    for key, value in raw.items():
        spec = OPTION_SPECS.get(key)
        if spec is None:
            raise HTTPException(status_code=400, detail=f"unsupported option: {key}")
        types, lo, hi = spec
        # bool 是 int 的子类，显式排除
        if isinstance(value, bool) or not isinstance(value, types):
            expect = "integer" if types == (int,) else "number"
            raise HTTPException(status_code=400,
                                detail=f"option {key} must be of type {expect}")
        if not (lo <= value <= hi):
            raise HTTPException(status_code=400,
                                detail=f"option {key} out of range [{lo}, {hi}]")
        options[key] = value

    config = {
        key: str(options[key]) if key in options else "default"
        for key in CONFIG_LABELS
    }
    return options, config


async def _stream_chat(api_key, tier, model, messages, ollama_payload,
                       config=None, include_usage=False,
                       rate_limit_backend="memory"):
    """把 Ollama 的 NDJSON 流转换成 OpenAI SSE 流。"""
    config = config or {key: "default" for key in CONFIG_LABELS}
    completion_id = _new_completion_id()
    start = time.time()
    final_chunk = {}
    status = 200

    INFLIGHT.inc()
    try:
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                async with client.stream(
                    "POST", f"{OLLAMA_URL}/api/chat", json=ollama_payload
                ) as resp:
                    if resp.status_code != 200:
                        status = 502
                        UPSTREAM_ERRORS_TOTAL.labels(model=model).inc()
                        detail = (await resp.aread()).decode(errors="replace")
                        yield _sse({"error": {
                            "message": f"Ollama returned {resp.status_code}: {detail[:200]}",
                            "type": "upstream_error",
                        }})
                        yield "data: [DONE]\n\n"
                        return

                    async for line in resp.aiter_lines():
                        line = line.strip()
                        if not line:
                            continue
                        chunk = json.loads(line)
                        content = chunk.get("message", {}).get("content", "")
                        if content:
                            yield _sse(_stream_chunk(completion_id, model, content))
                        if chunk.get("done"):
                            final_chunk = chunk
                            break
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            status = 502
            UPSTREAM_ERRORS_TOTAL.labels(model=model).inc()
            yield _sse({"error": {"message": str(exc), "type": "upstream_error"}})
            yield "data: [DONE]\n\n"
            return

        # 结束分片 + 结束标志
        yield _sse(_stream_chunk(completion_id, model, "", finish_reason="stop"))

        # OpenAI 兼容：stream_options.include_usage=true 时在 [DONE] 前回一个
        # 空 choices 的 usage 分片，并附带网关计算的 ttft/tps（供调优实验采集）
        if include_usage and status == 200 and final_chunk:
            um = compute_metrics(final_chunk)
            yield _sse({
                "id": completion_id,
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [],
                "usage": {
                    "prompt_tokens": um["prompt_tokens"],
                    "completion_tokens": um["completion_tokens"],
                    "total_tokens": um["prompt_tokens"] + um["completion_tokens"],
                },
                "metrics": {"ttft": um["ttft"], "tps": um["tps"]},
            })
        yield "data: [DONE]\n\n"
    finally:
        INFLIGHT.dec()
        wall = round(time.time() - start, 3)
        record_request(model=model, tier=tier, status=status,
                       stream=True, duration=wall)

        # 流结束后补记指标与日志
        metrics = compute_metrics(final_chunk)
        if status == 200:
            record_usage(model, tier, metrics, config=config)
        gpu_util, gpu_mem = current_gpu()
        _persist({
            "api_key": api_key,
            "tier": tier,
            "model": model,
            "messages_count": len(messages),
            "stream": True,
            "status": status,
            "wall_time": wall,
            "options": ollama_payload.get("options", {}),
            **config,                        # num_ctx / num_gpu / num_batch 标签
            "rate_limit_backend": rate_limit_backend,
            "instance": GATEWAY_INSTANCE,
            **metrics,
            "gpu_util": gpu_util,
            "gpu_mem_mb": gpu_mem,
        }, event="request.completed" if status == 200 else "request.error")


@app.post("/v1/chat/completions")
async def chat_completions(
    request: Request,
    response: Response,
    x_api_key: str = Header(...)
):
    # 1. 鉴权
    try:
        tier = await verify_api_key(x_api_key)
    except HTTPException:
        AUTH_FAILURES_TOTAL.inc()
        record_request(model="unknown", tier="unknown", status=401)
        _persist({"api_key": x_api_key, "tier": "unknown", "model": "unknown",
                  "status": 401, "instance": GATEWAY_INSTANCE,
                  "error": "invalid api key"}, event="request.auth_failed")
        raise

    # 2. 限流：Redis 固定窗口计数，多实例共享同一份额度；
    #    Redis 不可用时按 REDIS_FAIL_OPEN 降级（放行并改用进程内计数 / 直接拒绝）
    rl = await rate_limiter.check(x_api_key, tier, RATE_LIMIT[tier])
    # 非流式响应的限流头；X-Gateway-Instance 标明本次请求落在哪个实例，
    # 多实例部署时用它验证 nginx 负载均衡与故障转移（curl -i 即可看到）
    response.headers.update(rl.headers())
    response.headers["X-Gateway-Instance"] = GATEWAY_INSTANCE
    if not rl.allowed:
        RATE_LIMITED_TOTAL.labels(tier=tier).inc()
        record_request(model="unknown", tier=tier, status=429)
        _persist({"api_key": x_api_key, "tier": tier, "model": "unknown",
                  "status": 429, "instance": GATEWAY_INSTANCE,
                  "rate_limit_backend": rl.backend,
                  "error": f"rate limit exceeded ({rl.backend})"},
                 event="request.rate_limited")
        raise HTTPException(status_code=429, detail="Rate limit exceeded",
                            headers={**rl.headers(),
                                     "X-Gateway-Instance": GATEWAY_INSTANCE})

    # 3. 解析请求
    try:
        body = await request.json()
    except Exception:
        record_request(model="unknown", tier=tier, status=400)
        _persist({"api_key": x_api_key, "tier": tier, "model": "unknown",
                  "status": 400, "instance": GATEWAY_INSTANCE,
                  "rate_limit_backend": rl.backend,
                  "error": "request body is not valid JSON"}, event="request.error")
        raise HTTPException(status_code=400, detail="Request body must be valid JSON")

    model = resolve_model(body.get("model") or DEFAULT_MODEL)
    messages = body.get("messages", [])
    stream = bool(body.get("stream", False))
    try:
        options, config = _parse_options(body)  # Ollama 参数透传（白名单校验）
    except HTTPException as exc:
        record_request(model=model, tier=tier, status=400)
        _persist({"api_key": x_api_key, "tier": tier, "model": model,
                  "status": 400, "instance": GATEWAY_INSTANCE,
                  "rate_limit_backend": rl.backend,
                  "error": str(exc.detail)}, event="request.error")
        raise
    stream_options = body.get("stream_options") or {}
    include_usage = (isinstance(stream_options, dict)
                     and bool(stream_options.get("include_usage")))

    ollama_payload = {
        "model": model,
        "messages": messages,
        "stream": stream,
    }
    if options:
        ollama_payload["options"] = options

    # 4. 流式：转换为 OpenAI SSE 直接透传
    if stream:
        return StreamingResponse(
            _stream_chat(x_api_key, tier, model, messages, ollama_payload,
                         config=config, include_usage=include_usage,
                         rate_limit_backend=rl.backend),
            media_type="text/event-stream",
            headers={**rl.headers(), "X-Gateway-Instance": GATEWAY_INSTANCE},
        )

    # 5. 非流式：转发到 Ollama
    INFLIGHT.inc()
    start = time.time()
    status = 200
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            try:
                resp = await client.post(f"{OLLAMA_URL}/api/chat", json=ollama_payload)
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                status = 502
                UPSTREAM_ERRORS_TOTAL.labels(model=model).inc()
                detail = exc.response.text[:200]
                raise HTTPException(
                    status_code=502,
                    detail=f"Ollama returned {exc.response.status_code}: {detail}",
                )
            except httpx.HTTPError as exc:
                status = 502
                UPSTREAM_ERRORS_TOTAL.labels(model=model).inc()
                raise HTTPException(
                    status_code=502,
                    detail=f"Ollama unreachable at {OLLAMA_URL}: {exc}",
                )
            ollama_resp = resp.json()

        wall = time.time() - start

        # 6. 采集指标
        metrics = compute_metrics(ollama_resp)
        gpu_util, gpu_mem = current_gpu()
        record_usage(model, tier, metrics, config=config)

        # 7. 记录日志 + 历史 + 事件
        _persist({
            "api_key": x_api_key,
            "tier": tier,
            "model": model,
            "messages_count": len(messages),
            "stream": False,
            "status": status,
            "wall_time": round(wall, 3),
            "options": options,
            **config,                        # num_ctx / num_gpu / num_batch 标签
            "rate_limit_backend": rl.backend,
            "instance": GATEWAY_INSTANCE,
            **metrics,
            "gpu_util": gpu_util,
            "gpu_mem_mb": gpu_mem,
        })

        # 8. 返回 OpenAI 兼容格式
        return {
            "id": _new_completion_id(),
            "object": "chat.completion",
            "model": model,
            "usage": {
                "prompt_tokens": metrics["prompt_tokens"],
                "completion_tokens": metrics["completion_tokens"],
                "total_tokens": metrics["prompt_tokens"] + metrics["completion_tokens"],
            },
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": ollama_resp.get("message", {}).get("content", ""),
                },
                "finish_reason": "stop",
            }],
        }
    finally:
        INFLIGHT.dec()
        elapsed = round(time.time() - start, 3)
        record_request(model=model, tier=tier, status=status,
                       stream=False, duration=time.time() - start)
        if status != 200:
            # 上游失败也要留痕：文件日志 + 历史库 + Kafka 事件
            _persist({"api_key": x_api_key, "tier": tier, "model": model,
                      "messages_count": len(messages), "stream": False,
                      "status": status, "wall_time": elapsed,
                      "options": options, **config,
                      "rate_limit_backend": rl.backend,
                      "instance": GATEWAY_INSTANCE,
                      "error": "upstream error"}, event="request.error")


@app.get("/health")
async def health():
    ollama_up = False
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{OLLAMA_URL}/api/tags")
            ollama_up = resp.status_code == 200
    except httpx.HTTPError:
        ollama_up = False

    return {
        "status": "ok" if ollama_up else "degraded",
        "ollama": "up" if ollama_up else "down",
        "instance": GATEWAY_INSTANCE,
        "rate_limit_backend": rate_limiter.active_backend(),
        "history_backend": history.status()["backend"],
        "kafka_connected": events.connected,
    }


@app.get("/infra")
async def infra_status():
    """三大基础设施的运行状态（限流后端 / 历史库 / 事件总线，含降级与积压计数）。"""
    return {
        "instance": GATEWAY_INSTANCE,
        "rate_limit": rate_limiter.status(),
        "history": history.status(),
        "events": events.status(),
    }


@app.get("/history")
async def history_recent(limit: int = 20, model: str | None = None,
                         api_key: str | None = None):
    """按时间倒序返回最近的请求历史（数据来自 SQLite / Postgres）。"""
    limit = max(1, min(limit, 200))
    rows = await history.recent(limit=limit, model=model, api_key=api_key)
    return {
        "count": len(rows),
        "backend": history.status()["backend"],
        "items": rows,
    }


@app.get("/history/stats")
async def history_stats(window: int = 3600):
    """窗口内聚合统计：请求数 / 错误数 / 平均与 P95 延迟 / token 用量 / 分模型。"""
    return await history.stats(window_sec=max(1, window))


@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus 抓取端点（包含自定义指标 + process_* / python_* 运行时指标）。"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)