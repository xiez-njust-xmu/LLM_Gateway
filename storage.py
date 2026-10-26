"""请求历史存储：SQLite（默认，标准库 sqlite3）或 PostgreSQL（psycopg 3）。

写路径是异步、批量的：请求只把记录 `submit()` 进内存队列（非阻塞），后台任务按
`HISTORY_BATCH_SIZE` 行或 `HISTORY_FLUSH_INTERVAL` 秒聚合成一次多行 INSERT。
推理主链路因此不会被数据库抖动拖慢；队列满时宁可丢弃历史（计入
`gateway_history_dropped_total`）也不阻塞请求。

读路径（`/history`、`/history/stats`）在线程里按需建连接：SQLite 开 WAL 让读写
互不阻塞；Postgres 直接用 psycopg 同步驱动 + `asyncio.to_thread`，不再引入
asyncpg，避免维护两套 SQL 方言（差异只收敛在占位符 `?` / `%s`）。

DATABASE_URL 示例：
    sqlite:///./logs/gateway.db                  # 默认（零依赖）
    postgresql://gw:gw@localhost:5433/gateway    # 生产建议
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
import time

from prometheus_metrics import (
    HISTORY_DROPPED_TOTAL,
    HISTORY_ROWS,
    HISTORY_UP,
    HISTORY_WRITE_ERRORS_TOTAL,
    HISTORY_WRITES_TOTAL,
)

log = logging.getLogger("gateway.storage")

#: 历史表的列（顺序即 INSERT 顺序；id 由数据库自增，不在此列）
COLUMNS = (
    "ts", "api_key", "tier", "model", "status", "stream", "messages_count",
    "wall_time", "ttft", "tps", "prompt_tokens", "completion_tokens",
    "gpu_util", "gpu_mem_mb", "num_ctx", "num_gpu", "num_batch",
    "rate_limit_backend", "instance", "error",
)

_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS request_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    api_key TEXT, tier TEXT, model TEXT, status INTEGER, stream INTEGER,
    messages_count INTEGER, wall_time REAL, ttft REAL, tps REAL,
    prompt_tokens INTEGER, completion_tokens INTEGER,
    gpu_util INTEGER, gpu_mem_mb INTEGER,
    num_ctx TEXT, num_gpu TEXT, num_batch TEXT,
    rate_limit_backend TEXT, instance TEXT, error TEXT
);
CREATE INDEX IF NOT EXISTS idx_history_ts ON request_history (ts);
CREATE INDEX IF NOT EXISTS idx_history_model_ts ON request_history (model, ts);
"""

_PG_DDL = """
CREATE TABLE IF NOT EXISTS request_history (
    id BIGSERIAL PRIMARY KEY,
    ts DOUBLE PRECISION NOT NULL,
    api_key TEXT, tier TEXT, model TEXT, status INTEGER, stream SMALLINT,
    messages_count INTEGER, wall_time DOUBLE PRECISION, ttft DOUBLE PRECISION,
    tps DOUBLE PRECISION, prompt_tokens INTEGER, completion_tokens INTEGER,
    gpu_util INTEGER, gpu_mem_mb INTEGER,
    num_ctx TEXT, num_gpu TEXT, num_batch TEXT,
    rate_limit_backend TEXT, instance TEXT, error TEXT
);
CREATE INDEX IF NOT EXISTS idx_history_ts ON request_history (ts);
CREATE INDEX IF NOT EXISTS idx_history_model_ts ON request_history (model, ts);
"""


def _num(value, cast):
    """把任意值转成数字，失败返回 None（历史写入不该因为脏数据整批失败）。"""
    if value is None or value == "":
        return None
    try:
        return cast(value)
    except (TypeError, ValueError):
        return None


def normalise(record: dict) -> tuple:
    """把请求记录整理成与 COLUMNS 对齐的一行（缺失字段写 NULL）。"""
    row = (
        _num(record.get("ts"), float) or time.time(),
        record.get("api_key"),
        record.get("tier"),
        record.get("model"),
        _num(record.get("status"), int),
        1 if record.get("stream") else 0,
        _num(record.get("messages_count"), int),
        _num(record.get("wall_time"), float),
        _num(record.get("ttft"), float),
        _num(record.get("tps"), float),
        _num(record.get("prompt_tokens"), int),
        _num(record.get("completion_tokens"), int),
        _num(record.get("gpu_util"), int),
        _num(record.get("gpu_mem_mb"), int),
        record.get("num_ctx"),
        record.get("num_gpu"),
        record.get("num_batch"),
        record.get("rate_limit_backend"),
        record.get("instance"),
        record.get("error"),
    )
    return row


class _SqliteBackend:
    """SQLite 后端：每个线程一条连接，WAL 模式，读不阻塞写。"""

    name = "sqlite"
    placeholder = "?"

    def __init__(self, path: str):
        self.path = path
        self._local = threading.local()

    # --- 连接管理 ---
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            directory = os.path.dirname(os.path.abspath(self.path))
            os.makedirs(directory, exist_ok=True)
            conn = sqlite3.connect(self.path, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
        return conn

    def ensure_schema(self) -> None:
        conn = self._conn()
        conn.executescript(_SQLITE_DDL)
        conn.commit()

    def insert_many(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        conn = self._conn()
        sql = (f"INSERT INTO request_history ({', '.join(COLUMNS)}) "
               f"VALUES ({', '.join(['?'] * len(COLUMNS))})")
        conn.executemany(sql, rows)
        conn.commit()
        return len(rows)

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        cur = self._conn().execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def scalar(self, sql: str, params: tuple = ()):
        row = self._conn().execute(sql, params).fetchone()
        return row[0] if row else None

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None


class _PostgresBackend:
    """PostgreSQL 后端：psycopg 3 同步驱动，调用方统一用 `asyncio.to_thread` 包。"""

    name = "postgres"
    placeholder = "%s"

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._local = threading.local()

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None or conn.closed:
            import psycopg  # 延迟导入：不用 Postgres 时无需安装
            conn = psycopg.connect(self.dsn, autocommit=True)
            self._local.conn = conn
        return conn

    def ensure_schema(self) -> None:
        with self._conn().cursor() as cur:
            for stmt in _PG_DDL.split(";"):
                if stmt.strip():
                    cur.execute(stmt)

    def insert_many(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        sql = (f"INSERT INTO request_history ({', '.join(COLUMNS)}) "
               f"VALUES ({', '.join(['%s'] * len(COLUMNS))})")
        with self._conn().cursor() as cur:
            cur.executemany(sql, rows)
        return len(rows)

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._conn().cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def scalar(self, sql: str, params: tuple = ()):
        with self._conn().cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:                              # noqa: BLE001
                pass
            self._local.conn = None


def _backend_from_dsn(dsn: str):
    """按 DSN 前缀选择后端；相对路径的 SQLite 文件按项目目录解析。"""
    if dsn.startswith("sqlite"):
        path = dsn.split("///", 1)[1] if "///" in dsn else dsn.split("//", 1)[-1]
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
        return _SqliteBackend(path)

    if dsn.startswith(("postgresql://", "postgres://", "postgresql+psycopg://")):
        return _PostgresBackend(dsn.replace("+psycopg", ""))

    raise ValueError(f"不支持的 DATABASE_URL: {dsn}")


def _mask_dsn(dsn: str) -> str:
    """隐藏 DSN 里的密码，供 /infra 状态接口展示。"""
    if "@" in dsn and "//" in dsn:
        head, tail = dsn.split("//", 1)
        cred, host = tail.split("@", 1)
        if ":" in cred:
            cred = cred.split(":", 1)[0] + ":***"
        return f"{head}//{cred}@{host}"
    return dsn


class RequestHistory:
    """请求历史门面：`submit()` 非阻塞入队，后台任务批量落库。

    对外只暴露 submit / recent / stats / close / status，main.py 无需感知
    后端是 SQLite 还是 Postgres。
    """

    def __init__(self, dsn: str, batch_size: int = 20, flush_interval: float = 1.0,
                 queue_size: int = 5000):
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._backend = None
        self._writer: asyncio.Task | None = None
        self._written = 0
        self.error = ""

    # --- 生命周期 -----------------------------------------------------------

    async def start(self) -> None:
        try:
            self._backend = _backend_from_dsn(self.dsn)
            await asyncio.to_thread(self._backend.ensure_schema)
        except Exception as exc:                           # noqa: BLE001
            self.error = f"{type(exc).__name__}: {exc}"
            self._backend = None
            HISTORY_UP.labels(backend="none").set(0)
            log.warning("请求历史不可用（%s），/history 将返回空并继续计数丢弃", self.error)
            return

        HISTORY_UP.labels(backend=self._backend.name).set(1)
        # 带标签的计数器先落一条 0 值序列，Grafana 面板才不会显示 "No data"
        HISTORY_WRITES_TOTAL.labels(backend=self._backend.name).inc(0)
        HISTORY_WRITE_ERRORS_TOTAL.labels(backend=self._backend.name).inc(0)
        HISTORY_ROWS.labels(backend=self._backend.name).set(0)
        self._writer = asyncio.create_task(self._writer_loop())
        log.info("请求历史: %s（批量 %d 行 / %.1fs）", _mask_dsn(self.dsn),
                 self.batch_size, self.flush_interval)

    async def close(self) -> None:
        if self._writer:
            self._writer.cancel()
            try:
                await self._writer
            except asyncio.CancelledError:
                pass
        if self._backend is None:
            return
        # 退出前把队列里剩下的记录刷完，避免丢最后几条
        remaining = []
        while True:
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            await self._flush(remaining)
        try:
            await asyncio.to_thread(self._backend.close)
        except Exception:                                  # noqa: BLE001
            pass

    # --- 写路径 -------------------------------------------------------------

    def submit(self, record: dict) -> None:
        """把一条记录交给后台写库（非阻塞；队列满则丢弃并计数）。"""
        if self._backend is None:
            HISTORY_DROPPED_TOTAL.inc()
            return
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            HISTORY_DROPPED_TOTAL.inc()

    async def _writer_loop(self) -> None:
        """攒批：等到第一条记录后尽量把队列里已有的都打包成一次 INSERT。"""
        while True:
            try:
                first = await asyncio.wait_for(self._queue.get(), self.flush_interval)
            except asyncio.TimeoutError:
                continue
            batch = [first]
            while len(batch) < self.batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await self._flush(batch)

    async def _flush(self, batch: list[dict]) -> None:
        backend = self._backend
        if backend is None:
            return
        rows = [normalise(record) for record in batch]
        try:
            await asyncio.to_thread(backend.insert_many, rows)
        except Exception as exc:                           # noqa: BLE001
            self.error = f"{type(exc).__name__}: {exc}"
            HISTORY_WRITE_ERRORS_TOTAL.labels(backend=backend.name).inc()
            HISTORY_UP.labels(backend=backend.name).set(0)
            try:                                           # 丢弃坏连接，下批重连
                await asyncio.to_thread(backend.close)
            except Exception:                              # noqa: BLE001
                pass
            log.warning("历史写库失败，丢弃 %d 条: %s", len(rows), self.error)
            return

        self.error = ""
        self._written += len(rows)
        HISTORY_UP.labels(backend=backend.name).set(1)
        HISTORY_WRITES_TOTAL.labels(backend=backend.name).inc(len(rows))
        HISTORY_ROWS.labels(backend=backend.name).set(self._written)

    # --- 读路径 -------------------------------------------------------------

    async def recent(self, limit: int = 20, model: str | None = None,
                     api_key: str | None = None) -> list[dict]:
        """最近 N 条请求历史（倒序），供 /history 排查。"""
        backend = self._backend
        if backend is None:
            return []
        ph = backend.placeholder
        where, params = [], []
        if model:
            where.append(f"model = {ph}")
            params.append(model)
        if api_key:
            where.append(f"api_key = {ph}")
            params.append(api_key)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        sql = (
            "SELECT id, ts, api_key, tier, model, status, stream, messages_count,"
            " wall_time, ttft, tps, prompt_tokens, completion_tokens,"
            " gpu_util, gpu_mem_mb, num_ctx, num_gpu, num_batch,"
            " rate_limit_backend, instance, error"
            f" FROM request_history {clause} ORDER BY id DESC LIMIT {ph}"
        )
        params.append(int(limit))
        rows = await asyncio.to_thread(backend.query, sql, tuple(params))
        return [_decorate(row) for row in rows]

    async def stats(self, window_sec: int = 3600) -> dict:
        """窗口内的聚合统计（/history/stats）。"""
        backend = self._backend
        if backend is None:
            return {"enabled": False, "backend": "none", "error": self.error,
                    "window_seconds": window_sec, "totals": {}, "by_model": []}
        ph = backend.placeholder
        since = time.time() - window_sec

        totals = (await asyncio.to_thread(backend.query,
            "SELECT COUNT(*) AS requests,"
            " AVG(wall_time) AS avg_wall_time,"
            " AVG(ttft) AS avg_ttft, AVG(tps) AS avg_tps,"
            " SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS errors,"
            " SUM(prompt_tokens) AS prompt_tokens,"
            " SUM(completion_tokens) AS completion_tokens"
            f" FROM request_history WHERE ts >= {ph}", (since,)))[0]

        by_model = await asyncio.to_thread(backend.query,
            "SELECT model, COUNT(*) AS requests, AVG(ttft) AS avg_ttft,"
            " AVG(tps) AS avg_tps"
            f" FROM request_history WHERE ts >= {ph}"
            " GROUP BY model ORDER BY requests DESC", (since,))

        total_rows = await asyncio.to_thread(backend.scalar,
                                             "SELECT COUNT(*) FROM request_history")
        HISTORY_ROWS.labels(backend=backend.name).set(total_rows or 0)

        return {
            "enabled": True,
            "backend": backend.name,
            "window_seconds": window_sec,
            "totals": {
                "requests": int(totals["requests"] or 0),
                "errors": int(totals["errors"] or 0),
                "avg_wall_time": _round(totals["avg_wall_time"]),
                "avg_ttft": _round(totals["avg_ttft"]),
                "avg_tps": _round(totals["avg_tps"]),
                "p95_wall_time": _round(await self._percentile("wall_time", 0.95, since)),
                "p95_ttft": _round(await self._percentile("ttft", 0.95, since)),
                "prompt_tokens": int(totals["prompt_tokens"] or 0),
                "completion_tokens": int(totals["completion_tokens"] or 0),
            },
            "by_model": [
                {"model": row["model"], "requests": int(row["requests"] or 0),
                 "avg_ttft": _round(row["avg_ttft"]), "avg_tps": _round(row["avg_tps"])}
                for row in by_model
            ],
            "rows_total": int(total_rows or 0),
            "rows_this_instance": self._written,
        }

    async def _percentile(self, field: str, q: float, since: float):
        """可移植的百分位：COUNT + ORDER BY ... OFFSET（field 由内部常量传入）。"""
        backend = self._backend
        ph = backend.placeholder
        n = await asyncio.to_thread(backend.scalar,
            f"SELECT COUNT(*) FROM request_history WHERE ts >= {ph}"
            f" AND {field} IS NOT NULL", (since,))
        n = int(n or 0)
        if n == 0:
            return None
        offset = min(int(n * q), n - 1)
        return await asyncio.to_thread(backend.scalar,
            f"SELECT {field} FROM request_history WHERE ts >= {ph}"
            f" AND {field} IS NOT NULL ORDER BY {field} LIMIT 1 OFFSET {offset}",
            (since,))

    def status(self) -> dict:
        return {
            "enabled": self._backend is not None,
            "backend": self._backend.name if self._backend else "none",
            "dsn": _mask_dsn(self.dsn),
            "queued": self._queue.qsize(),
            "written": self._written,
            "batch_size": self.batch_size,
            "flush_interval": self.flush_interval,
            "error": self.error,
        }


def _round(value, digits: int = 3):
    """数值取整；None 原样返回（JSON 里保持 null 而不是 0）。"""
    return round(float(value), digits) if value is not None else None


def _decorate(row: dict) -> dict:
    """把 epoch 时间戳转成可读 ISO 字符串，并补上 total_tokens。"""
    ts = row.get("ts")
    if isinstance(ts, (int, float)):
        row["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts))
    row["total_tokens"] = (row.get("prompt_tokens") or 0) + (row.get("completion_tokens") or 0)
    return row
