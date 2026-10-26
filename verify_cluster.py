#!/usr/bin/env python
"""多实例 + nginx 负载均衡验收脚本。

脚本会临时拉起 3 个网关实例（默认 :8100/:8101/:8102）和一个独立前缀的 nginx
（默认 :8180，不会动你正在跑的 8080），把整条链路验一遍：

1. 启动/健康：3 个实例 + nginx 配置校验 + `/lb-health`；
2. 负载均衡：反复经 LB 请求，看响应里的实例名是否在 3 个实例间轮换、是否均衡；
3. 响应头透传：`X-Gateway-Instance`、`X-RateLimit-*` 能否穿过 nginx；
4. 流式（SSE）：经 LB 能否正常收到 `data: [DONE]`，以及**是否被 nginx 缓冲**
   （用「首字节时间 vs 总时长」判断，被缓冲时两者几乎相等）；
5. 共享限流：经 LB 用 free key 连发 12 次，跨 3 个实例只应放行 10 次；
6. 多实例写历史：数据库里应出现多个不同 instance；
7. **故障转移**：杀掉一个实例，nginx 按 max_fails 自动摘除，请求仍全部成功；
   实例重新拉起后 fail_timeout 过去又会自动加回；
8. 观测隔离：`/metrics` 直连每个实例都 200，而经 LB 被明确拒绝。

用法：
    python verify_cluster.py                     # 全量（约 1 分钟）
    python verify_cluster.py --instances 3 --port 8100 --lb-port 8180
    python verify_cluster.py --keep              # 跑完保留实例与 nginx（排障用）
    python verify_cluster.py --skip-failover     # 不杀实例（跳过第 7 项）

依赖：运行中的 Ollama（发真实请求）、可选的 Redis（共享限流）。
nginx 二进制缺失时会自动调用 scripts/nginx.sh install 下载。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import time

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
FREE_KEY = "sk-test-001"           # free 档：RATE_LIMIT["free"] = 10 次 / 窗口
FREE_LIMIT = 10
PRO_KEY = "sk-prod-001"
CHAT = {"model": "qwen0.5b", "options": {"num_predict": 4, "num_ctx": 512}}

_results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    _results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


class GatewayInstance:
    """一个临时网关进程；每个实例只有端口 / 实例名 / 日志文件不同。"""

    def __init__(self, port: int, env: dict):
        self.port = port
        self.env = env
        self.instance_name = f"gateway-{port}"
        self.proc: subprocess.Popen | None = None
        self.log = os.path.join(BASE_DIR, "logs", f"verify_cluster_{port}.log")
        self.base = f"http://localhost:{port}"

    def start(self, timeout: float = 40.0) -> bool:
        env = {**os.environ, **self.env,
               "SERVER_PORT": str(self.port),
               "GATEWAY_INSTANCE": self.instance_name,
               "GATEWAY_LOG_FILE": self.log}
        os.makedirs(os.path.dirname(self.log), exist_ok=True)
        handle = open(self.log, "a", encoding="utf-8")
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


class NginxLB:
    """用 scripts/nginx.sh 管理一个独立前缀的 nginx（端口 / pid / 日志都独立）。"""

    def __init__(self, port: int, upstreams: list[str], home: str):
        self.port = port
        self.home = home
        self.env = {**os.environ, "NGINX_HOME": home, "LB_PORT": str(port),
                    "GATEWAY_INSTANCES": ",".join(upstreams)}
        self.base = f"http://localhost:{port}"

    def _run(self, action: str) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", os.path.join(BASE_DIR, "scripts", "nginx.sh"), action],
                              cwd=BASE_DIR, env=self.env, capture_output=True,
                              text=True, timeout=180)

    def prepare(self) -> None:
        """复用已安装好的 nginx 二进制，避免每次都重新下载 deb。"""
        dest = os.path.join(self.home, "bin", "nginx")
        src = os.path.expanduser("~/opt/nginx/bin/nginx")
        if os.path.exists(src) and not os.path.exists(dest):
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy2(src, dest)

    def install(self) -> subprocess.CompletedProcess:
        self.prepare()
        return self._run("install")

    def test(self) -> subprocess.CompletedProcess:
        return self._run("test")

    def start(self) -> subprocess.CompletedProcess:
        return self._run("start")

    def stop(self) -> None:
        self._run("stop")

    def health(self, timeout: float = 5.0) -> requests.Response:
        return requests.get(f"{self.base}/health", timeout=timeout)

    def lb_health(self, timeout: float = 5.0) -> requests.Response:
        return requests.get(f"{self.base}/lb-health", timeout=timeout)

    def chat(self, api_key: str = PRO_KEY, stream: bool = False, **extra) -> requests.Response:
        payload = {**CHAT, "messages": [{"role": "user", "content": "只回复 OK"}],
                   "stream": stream, **extra}
        return requests.post(f"{self.base}/v1/chat/completions", json=payload,
                             headers={"X-API-Key": api_key}, timeout=180)

    def access_log(self) -> list[dict]:
        path = os.path.join(self.home, "logs", "nginx-access.log")
        rows: list[dict] = []
        try:
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            pass
        return rows


def hit_instance(resp: requests.Response) -> str:
    """实例名优先取响应头，其次取 /health 响应体里的 instance 字段。"""
    name = resp.headers.get("x-gateway-instance")
    if name:
        return name
    try:
        return resp.json().get("instance", "")
    except ValueError:
        return ""


def sample(lb: NginxLB, count: int = 12) -> dict[str, int]:
    """经 LB 连续打 count 次 /health，统计每个实例被命中的次数。"""
    hits: dict[str, int] = {}
    for _ in range(count):
        resp = lb.health()
        name = hit_instance(resp)
        hits[name] = hits.get(name, 0) + 1
    return hits


def probe(lb: NginxLB, count: int = 8) -> list[tuple[int, str]]:
    """经 LB 连续打 count 次 /health，返回每次的 (状态码, 实例名)。"""
    out: list[tuple[int, str]] = []
    for _ in range(count):
        resp = lb.health()
        out.append((resp.status_code, hit_instance(resp)))
    return out


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


def stream_result(base_url: str, num_predict: int = 64,
                  num_ctx: int = 512) -> tuple[float, float, str]:
    """发一次流式请求，返回 (首字节耗时, 总耗时, 收到的文本)。

    num_ctx 必须与普通请求保持一致：Ollama 在上下文长度变化时会重载模型，
    首字节会被这次重载拖到几秒，看起来像是「被 nginx 缓冲了」。
    """
    start = time.time()
    first = -1.0
    text = ""
    payload = {"model": "qwen0.5b", "stream": True,
               "options": {"num_predict": num_predict, "num_ctx": num_ctx},
               "messages": [{"role": "user", "content": "从 1 数到 20"}]}
    with requests.post(f"{base_url}/v1/chat/completions", json=payload,
                       headers={"X-API-Key": PRO_KEY}, stream=True, timeout=180) as resp:
        for chunk in resp.iter_content(chunk_size=1):
            if chunk:
                if first < 0:
                    first = time.time() - start
                text += chunk.decode("utf-8", "ignore")
    return first, time.time() - start, text


def port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def default_database_url() -> str:
    """多实例写同一张表，默认优先用 PostgreSQL（与 scripts/cluster.sh 的默认一致）。"""
    env = os.getenv("DATABASE_URL")
    if env:
        return env
    if port_open(5433):
        return "postgresql://gw:gw@localhost:5433/gateway"
    return f"sqlite:///{os.path.join(BASE_DIR, 'logs', 'gateway.db')}"


def redis_alive(url: str) -> bool:
    try:
        import redis
        client = redis.Redis.from_url(url, socket_connect_timeout=2)
        client.ping()
        client.close()
        return True
    except Exception:                                      # noqa: BLE001
        return False


# --- 各项检查 -----------------------------------------------------------------

def check_health(instances: list[GatewayInstance], lb: NginxLB) -> None:
    print("\n[1] 启动与健康")
    for inst in instances:
        body = requests.get(f"{inst.base}/health", timeout=5).json()
        check(f"{inst.instance_name} 健康",
              body.get("status") in ("ok", "degraded"),
              f"ollama={body.get('ollama')} 限流={body.get('rate_limit_backend')} "
              f"历史={body.get('history_backend')} kafka={body.get('kafka_connected')}")
    result = lb.test()
    out = (result.stdout or result.stderr or "").strip().splitlines()
    check("nginx 配置校验通过（nginx -t）", result.returncode == 0, out[-1] if out else "")
    check("LB 健康端点可用", lb.lb_health().status_code == 200, f"{lb.base}/lb-health")
    check("经 LB 能拿到实例名（说明请求确实转给了后端实例）",
          bool(hit_instance(lb.health())))


def check_balancing(lb: NginxLB, instances: list[GatewayInstance], probes: int = 12) -> None:
    print("\n[2] 负载均衡分发")
    resp = lb.chat(api_key=PRO_KEY)
    check("非流式请求经 LB 成功", resp.status_code == 200, f"instance={hit_instance(resp)}")
    check("响应头带 X-Gateway-Instance（nginx 原样透传）",
          bool(resp.headers.get("x-gateway-instance")),
          resp.headers.get("x-gateway-instance", ""))
    check("限流响应头经 LB 透传", "x-ratelimit-remaining" in resp.headers,
          f"limit={resp.headers.get('x-ratelimit-limit')} "
          f"remaining={resp.headers.get('x-ratelimit-remaining')}")

    hits = sample(lb, probes)
    print(f"        经 LB 打 {probes} 次 /health 的分布: {hits}")
    got = {k: v for k, v in hits.items() if k}
    check(f"全部 {len(instances)} 个实例都分到了流量", len(got) == len(instances),
          f"命中 {len(got)}/{len(instances)}: {sorted(got)}")
    counts = sorted(got.values())
    check("分发比较均衡（最多与最少相差 ≤ 2 次）",
          bool(counts) and counts[-1] - counts[0] <= 2, f"各实例次数={counts}")


def check_streaming(lb: NginxLB, instances: list[GatewayInstance]) -> None:
    print("\n[3] 流式（SSE）经 nginx")
    first_lb, total_lb, text = stream_result(lb.base)
    check("收到 SSE 结束标记 data: [DONE]", "[DONE]" in text,
          f"data 块数={text.count('data:')}")
    check("流式内容不空", '"content"' in text)

    first_direct, total_direct, _ = stream_result(instances[0].base)
    ratio = first_lb / total_lb if total_lb > 0 else 1.0
    print(f"        经 LB : 首字节 {first_lb:.3f}s / 总时长 {total_lb:.3f}s")
    print(f"        直连实例: 首字节 {first_direct:.3f}s / 总时长 {total_direct:.3f}s")
    check("未被 nginx 缓冲（首字节远早于总时长，且与直连相当）",
          first_lb >= 0 and ratio < 0.8 and first_lb <= max(0.3, first_direct + 0.3),
          f"LB 首字节/总时长={ratio:.0%}（若 proxy_buffering on，这个比例会接近 100%）")


def check_rate_limit_shared(lb: NginxLB, instances: list[GatewayInstance],
                            redis_url: str, redis_ok: bool) -> None:
    backend = requests.get(f"{instances[0].base}/health", timeout=5).json() \
        .get("rate_limit_backend")
    print(f"\n[4] 跨实例共享限流（限流后端={backend}）")
    if not redis_ok:
        print("  [SKIP] Redis 不可用，跳过共享限流检查（bash scripts/infra.sh start 可拉起）")
        return
    reset_rate_limit(redis_url)
    codes: list[int] = []
    rejecters: set[str] = set()
    for _ in range(FREE_LIMIT + 2):
        resp = lb.chat(api_key=FREE_KEY)
        codes.append(resp.status_code)
        if resp.status_code == 429:
            rejecters.add(hit_instance(resp))
    allowed, limited = codes.count(200), codes.count(429)
    if backend == "redis":
        check(f"跨 {len(instances)} 个实例只放行 {FREE_LIMIT} 次（额度真共享）",
              allowed == FREE_LIMIT, f"200={allowed} 429={limited} codes={codes}")
        check("超额请求被拒（429）", limited >= 1)
        check("429 响应也带 X-Gateway-Instance（知道是哪个实例拒的）",
              bool(rejecters), ",".join(sorted(rejecters)))
    else:
        check("内存后端下额度不共享（每个实例各算各的，12 次全放行）",
              allowed == FREE_LIMIT + 2, f"200={allowed} codes={codes}")


async def check_history(db_url: str) -> None:
    print("\n[5] 多实例写同一份请求历史")
    sys.path.insert(0, BASE_DIR)
    from storage import RequestHistory

    hist = RequestHistory(db_url, flush_interval=0.2)
    await hist.start()
    try:
        status = hist.status()
        if not check("数据库可连接", status["enabled"], status["error"] or status["dsn"]):
            return
        rows = await hist.recent(limit=200)
        seen = sorted({r.get("instance") for r in rows if r.get("instance")})
        check("历史里出现多个实例（多实例都在写同一张表）", len(seen) >= 2,
              f"instances={seen}")
        if rows:
            fields = ("ts", "model", "status", "stream", "instance", "wall_time",
                      "rate_limit_backend")
            print("        最新一行: "
                  + json.dumps({k: rows[0].get(k) for k in fields}, ensure_ascii=False))
    finally:
        await hist.close()


def check_failover(lb: NginxLB, instances: list[GatewayInstance]) -> None:
    print("\n[6] 故障转移（杀掉一个实例，看 nginx 摘除与自动加回）")
    victim = instances[0]
    healthy = {i.instance_name for i in instances[1:]}
    print(f"        停掉 {victim.instance_name}（模拟实例崩溃）")
    victim.stop()
    time.sleep(1)

    # 第一批：触发 max_fails，把故障实例摘掉。
    # 注意 nginx 会对 GET 自动换机重试，所以客户端看到的多半还是 200。
    probes = probe(lb, 12)
    codes = [c for c, _ in probes]
    names = {n for _, n in probes}
    check("故障期间客户端仍全部成功（nginx 自动换到下一个上游）",
          all(c == 200 for c in codes), f"codes={codes}")
    check("响应不再来自故障实例（请求都被转给存活实例）",
          victim.instance_name not in names, f"命中={sorted(names)}")

    retries = [r for r in lb.access_log() if "," in str(r.get("upstream_addr", ""))]
    check("访问日志可见自动换机（upstream_addr 有两个上游 / 状态 502,200）",
          bool(retries),
          json.dumps({k: retries[-1].get(k) for k in ("upstream_addr", "upstream_status",
                                                      "uri")}, ensure_ascii=False)
          if retries else "未捕捉到，可看 nginx-error.log 里的 upstream 报错")

    # 第二批：此时故障实例已被摘除，后续请求不应再出现「换机」痕迹，也不应出现 5xx
    before = len(lb.access_log())
    probes2 = probe(lb, 8)
    rows2 = lb.access_log()[before:]
    retries2 = [r for r in rows2 if "," in str(r.get("upstream_addr", "")) or
                str(r.get("upstream_status", "")).startswith("5")]
    check("摘除生效：后续 8 次请求不再触发换机（不再去连故障实例）",
          not retries2, f"新增 {len(rows2)} 条访问日志，其中换机/5xx {len(retries2)} 条")
    check("摘除后请求依旧全部成功",
          all(c == 200 for c, _ in probes2), f"codes={[c for c, _ in probes2]}")

    errlog = os.path.join(lb.home, "logs", "nginx-error.log")
    err_text = ""
    try:
        with open(errlog, encoding="utf-8", errors="replace") as fh:
            err_text = fh.read()
    except FileNotFoundError:
        pass
    markers = [m for m in ("connect() failed", "temporarily disabled",
                           "upstream timed out") if m in err_text]
    check("nginx-error.log 记录了上游不可用", bool(markers), f"命中关键字: {markers}")

    print(f"        重新拉起 {victim.instance_name}，fail_timeout=10s 过后应自动加回")
    if not victim.start():
        check("故障实例重新启动", False, victim.log)
        return
    back = False
    for _ in range(15):
        if victim.instance_name in sample(lb, 3):
            back = True
            break
        time.sleep(1)
    check("实例恢复后自动加回上游（无需改配置或 reload）", back,
          f"恢复前只有 {sorted(healthy)} 在服役")


def check_observability(lb: NginxLB, instances: list[GatewayInstance]) -> None:
    print("\n[7] 观测隔离（指标按实例直连抓取）")
    oks = 0
    for inst in instances:
        resp = requests.get(f"{inst.base}/metrics", timeout=10)
        if resp.status_code == 200 and b"gateway_requests_total" in resp.content:
            oks += 1
    check("每个实例的 /metrics 可直连抓取", oks == len(instances),
          f"{oks}/{len(instances)}（Prometheus 用 instance 标签区分实例）")
    resp = requests.get(f"{lb.base}/metrics", timeout=5)
    check("经 LB 的 /metrics 被明确拒绝", resp.status_code == 404,
          f"status={resp.status_code}（避免所有实例指标混成一个 instance）")


def check_access_log(lb: NginxLB) -> None:
    print("\n[8] nginx 访问日志（JSON，含 upstream_addr / instance）")
    rows = lb.access_log()
    upstreams = sorted({str(r.get("upstream_addr")) for r in rows if r.get("upstream_addr")})
    names = sorted({str(r.get("instance")) for r in rows if r.get("instance")})
    check("访问日志可逐行解析成 JSON", bool(rows), f"{len(rows)} 行")
    check("日志里出现多个上游地址", len(upstreams) >= 2, str(upstreams))
    check("日志里出现多个实例名", len(names) >= 2, str(names))
    if rows:
        print("        样例: " + json.dumps(rows[-1], ensure_ascii=False))


def main() -> int:
    ap = argparse.ArgumentParser(description="多实例网关 + nginx 负载均衡验收")
    ap.add_argument("--instances", type=int, default=3, help="实例数量（默认 3）")
    ap.add_argument("--port", type=int, default=8100, help="第一个实例端口（依此 +1）")
    ap.add_argument("--lb-port", type=int, default=8180, help="nginx 监听端口（默认 8180）")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--database-url", default=default_database_url())
    ap.add_argument("--skip-failover", action="store_true", help="不杀实例（跳过第 6 项）")
    ap.add_argument("--keep", action="store_true", help="跑完保留实例与 nginx（排障用）")
    args = ap.parse_args()

    ports = [args.port + i for i in range(args.instances)]
    nginx_home = os.path.join(BASE_DIR, "logs", "cluster", f"verify-nginx-{args.lb_port}")
    redis_ok = redis_alive(args.redis_url)

    common = {
        "DATABASE_URL": args.database_url,
        "RATE_LIMIT_WINDOW": "60",
        "REDIS_URL": args.redis_url,
        "RATE_LIMIT_BACKEND": "redis" if redis_ok else "memory",
        "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    }
    instances = [GatewayInstance(p, common) for p in ports]
    lb = NginxLB(args.lb_port, [f"127.0.0.1:{p}" for p in ports], nginx_home)

    print(f"实例      : {ports}")
    print(f"负载均衡  : nginx :{args.lb_port} -> {[f'127.0.0.1:{p}' for p in ports]}")
    print(f"Redis     : {args.redis_url}  ({'可用' if redis_ok else '不可用，退化为内存限流'})")
    print(f"数据库    : {args.database_url}")

    try:
        print(f"\n拉起 {args.instances} 个网关实例…")
        started = [inst.start() for inst in instances]
        if not check(f"{args.instances} 个实例启动成功", all(started),
                     f"日志 {os.path.join(BASE_DIR, 'logs')}/verify_cluster_*.log"):
            return 1
        res = lb.install()
        check("nginx 就绪（免 root 解包 / 复用已有二进制）", res.returncode == 0,
              (res.stdout or res.stderr).strip().splitlines()[-1] if (res.stdout or res.stderr) else "")
        res = lb.start()
        check("nginx 启动并渲染上游列表", res.returncode == 0,
              (res.stdout or "").strip().splitlines()[-1] if res.stdout else "")

        check_health(instances, lb)
        check_balancing(lb, instances)
        check_streaming(lb, instances)
        check_rate_limit_shared(lb, instances, args.redis_url, redis_ok)
        asyncio.run(check_history(args.database_url))
        if args.skip_failover:
            print("\n[6] 故障转移：已按 --skip-failover 跳过")
        else:
            check_failover(lb, instances)
        check_observability(lb, instances)
        check_access_log(lb)
    finally:
        if args.keep:
            print("\n(--keep) 实例与 nginx 保持运行，收尾命令：")
            print(f"  NGINX_HOME={nginx_home} LB_PORT={args.lb_port} bash scripts/nginx.sh stop")
        else:
            lb.stop()
            for inst in instances:
                inst.stop()

    failed = [name for name, ok, _ in _results if not ok]
    print("\n" + "=" * 66)
    print(f"验收结果: {len(_results) - len(failed)}/{len(_results)} 项通过")
    for name in failed:
        print(f"  FAIL  {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
