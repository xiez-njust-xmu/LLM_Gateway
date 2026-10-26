#!/usr/bin/env python
"""Ollama 参数调优实验：扫 num_ctx / num_gpu / num_batch，测量 TTFT、TPS、显存。

经网关 SSE（stream_options.include_usage）发请求：
- client_ttft : 发出请求 -> 收到首个内容分片的墙钟时间（端到端首字延迟）
- gw_ttft     : 网关计算的首 token 延迟 = Ollama load_duration + prompt_eval_duration
- gw_tps      : 网关计算的生成速度 = eval_count / eval_duration
- vram_mb     : 每次请求后 nvidia-smi 显存占用（MiB）

每组配置先发 1 次 warmup（options 变化会触发 Ollama runner 重载，产生冷启动
load_duration 尖峰，不计入统计），再测量 --runs 次。

产出：
- logs/tuning_experiment.jsonl          逐请求明细
- logs/tuning_experiment_summary.json   按配置聚合的汇总
- Prometheus 自动采集网关按 num_ctx/num_gpu/num_batch 标签暴露的 TTFT/TPS，
  Grafana「LLM Gateway 监控 -> Ollama 参数调优」行实时绘制曲线。

用法：
    python tuning_experiment.py                    # 完整扫描
    python tuning_experiment.py --sweeps ctx       # 只扫 num_ctx
    python tuning_experiment.py --runs 2 --dry-run
"""
import argparse
import json
import os
import statistics
import subprocess
import time

import requests

from config import MODEL_ROUTES, OLLAMA_URL

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8000").rstrip("/")
API_KEY = os.getenv("GATEWAY_API_KEY", "sk-prod-001")

# 固定参数（被扫描的那个参数会被覆盖）；num_predict 限制生成长度使各组耗时可比
BASE_OPTIONS = {
    "num_ctx": 2048,
    "num_gpu": -1,
    "num_batch": 512,
    "num_predict": 64,
    "temperature": 0,
}

# 扫描计划；gpu 组的 None 表示"半层 offload"，运行时按模型实际层数计算
SWEEPS = {
    "ctx": ("num_ctx", [512, 1024, 2048, 4096, 8192]),
    "gpu": ("num_gpu", [0, None, -1]),
    "batch": ("num_batch", [64, 128, 256, 512, 1024]),
}

_SENTENCE = ("performance tuning of local language models requires careful "
             "measurement because time to first token depends on prompt "
             "evaluation, model loading, memory bandwidth and compute. ")
# 约 240 词（~300 token），必须能放进最小 num_ctx=512（含模板与 64 个生成 token）
PROMPT = (" ".join([_SENTENCE] * 6)
          + "\n\nQuestion: reply to this with the single word OK.")


def gpu_mem_mb() -> int:
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.used",
             "--format=csv,noheader,nounits"], text=True)
        return int(out.strip().splitlines()[0])
    except Exception:
        return 0


def detect_layers(model_alias: str) -> int:
    """查 Ollama /api/show 拿模型 block 数（num_gpu 半层 offload 用）。"""
    real = MODEL_ROUTES.get(model_alias, model_alias)
    try:
        resp = requests.post(f"{OLLAMA_URL}/api/show", json={"model": real},
                             timeout=30)
        info = resp.json().get("model_info", {})
        for key, value in info.items():
            if key.endswith("block_count"):
                return int(value)
    except Exception:
        pass
    return 14  # qwen2.5:0.5b 兜底


def unload_model(model_alias: str) -> None:
    """让 Ollama 立刻卸载模型（keep_alive=0）。

    必要性：Ollama 0.35 里 num_gpu=-1（auto）与已加载 runner 的显式值比较时
    会判定"配置未变"而不重载（实测 12 -> -1、0 -> -1 的 load_duration=2ms，
    size_vram 原样保留），下一组配置会跑在上一组的 offload 状态上。
    每组开始前显式卸载，保证 warmup 一定是"用本组 options 的冷加载"。
    """
    real = MODEL_ROUTES.get(model_alias, model_alias)
    try:
        requests.post(f"{OLLAMA_URL}/api/chat",
                      json={"model": real,
                            "messages": [{"role": "user", "content": "x"}],
                            "stream": False, "keep_alive": 0}, timeout=60)
    except Exception as exc:
        print(f"  [警告] 卸载模型失败: {exc}")


def run_once(model: str, options: dict, timeout: float = 300) -> dict:
    """经网关发一次流式请求，返回测量记录。"""
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": PROMPT}],
        "stream": True,
        "stream_options": {"include_usage": True},
        "options": options,
    }
    headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
    url = f"{GATEWAY_URL}/v1/chat/completions"

    for attempt in (1, 2):
        t0 = time.time()
        client_ttft = None
        usage = None
        gw_metrics = None
        with requests.post(url, json=payload, headers=headers, stream=True,
                           timeout=timeout) as resp:
            if resp.status_code == 429 and attempt == 1:
                time.sleep(30)  # 网关限流，等一个窗口重试
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            for line in resp.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue
                data = line[6:]
                if data == "[DONE]":
                    break
                obj = json.loads(data)
                if "error" in obj:
                    raise RuntimeError(f"gateway error: {obj['error']}")
                choices = obj.get("choices") or []
                content = (choices[0].get("delta") or {}).get("content") if choices else None
                if content and client_ttft is None:
                    client_ttft = time.time() - t0
                if obj.get("usage"):
                    usage = obj["usage"]
                    gw_metrics = obj.get("metrics") or {}
        wall = time.time() - t0
        if usage is None:
            raise RuntimeError("流结束未收到 usage 分片？确认网关已支持 include_usage")
        return {
            "client_ttft": round(client_ttft or wall, 3),
            "wall_time": round(wall, 3),
            "gw_ttft": gw_metrics.get("ttft"),
            "gw_tps": gw_metrics.get("tps"),
            "prompt_tokens": usage["prompt_tokens"],
            "completion_tokens": usage["completion_tokens"],
            "vram_mb": gpu_mem_mb(),
        }
    raise RuntimeError("unreachable")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ollama 参数调优实验")
    ap.add_argument("--model", default="qwen0.5b", help="网关模型别名")
    ap.add_argument("--sweeps", default="ctx,gpu,batch",
                    help="逗号分隔，可选 ctx,gpu,batch")
    ap.add_argument("--runs", type=int, default=3, help="每组测量次数")
    ap.add_argument("--interval", type=float, default=1.0, help="请求间隔秒")
    ap.add_argument("--out", default=os.path.join(BASE_DIR, "logs", "tuning_experiment.jsonl"))
    ap.add_argument("--summary", default=os.path.join(BASE_DIR, "logs", "tuning_experiment_summary.json"))
    ap.add_argument("--dry-run", action="store_true", help="只打印实验计划")
    args = ap.parse_args()

    layers = detect_layers(args.model)
    plan = []
    for name in args.sweeps.split(","):
        name = name.strip()
        if name not in SWEEPS:
            raise SystemExit(f"未知 sweep: {name}（可选 {', '.join(SWEEPS)}）")
        param, values = SWEEPS[name]
        for value in values:
            if value is None:
                value = layers // 2
            plan.append((name, param, value))

    print(f"模型={args.model}  每组 warmup=1 + 测量={args.runs} 次  "
          f"总请求数={len(plan) * (1 + args.runs)}  模型层数={layers}")
    for name, param, value in plan:
        print(f"  [{name}] {param}={value}")
    if args.dry_run:
        return

    idle_vram = gpu_mem_mb()
    records = []
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    t_start = time.time()

    with open(args.out, "a", encoding="utf-8") as fp:
        def emit(rec: dict) -> dict:
            rec["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fp.flush()
            records.append(rec)
            print(f"  {rec['phase']:<10} client_ttft={rec['client_ttft']:.3f}s "
                  f"gw_ttft={rec['gw_ttft']}s tps={rec['gw_tps']} "
                  f"vram={rec['vram_mb']}MiB")
            return rec

        for name, param, value in plan:
            options = {**BASE_OPTIONS, param: value}
            print(f"\n=== {param}={value} ===")
            unload_model(args.model)  # 见 unload_model 注释：强制本组冷加载
            emit({**run_once(args.model, options), "sweep": name,
                  "param": param, "value": value, "phase": "warmup",
                  "options": options})
            for i in range(args.runs):
                rec = emit({**run_once(args.model, options), "sweep": name,
                            "param": param, "value": value,
                            "phase": f"measure#{i + 1}", "options": options})
                if i == 0 and name == "ctx":
                    need = rec["prompt_tokens"] + BASE_OPTIONS["num_predict"]
                    if need > value:
                        print(f"  [警告] prompt({rec['prompt_tokens']}) + num_predict"
                              f" 超过 num_ctx={value}，输出可能被截断")
                time.sleep(args.interval)

    # 汇总（只统计 measure，不含 warmup）
    summary = {"model": args.model, "elapsed_sec": round(time.time() - t_start, 1),
               "idle_vram_mb": idle_vram, "base_options": BASE_OPTIONS,
               "configs": {}}
    for name, param, value in plan:
        rows = [r for r in records if r["param"] == param and r["value"] == value
                and r["phase"].startswith("measure")]
        if not rows:
            continue
        summary["configs"][f"{param}={value}"] = {
            "sweep": name,
            "runs": len(rows),
            "client_ttft_mean": round(statistics.mean(r["client_ttft"] for r in rows), 3),
            "gw_ttft_mean": round(statistics.mean(r["gw_ttft"] for r in rows), 3),
            "tps_mean": round(statistics.mean(r["gw_tps"] for r in rows), 2),
            "vram_max_mb": max(r["vram_mb"] for r in rows),
            "prompt_tokens": rows[0]["prompt_tokens"],
        }
    with open(args.summary, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)

    print(f"\n===== 汇总（measure 均值，耗时 {summary['elapsed_sec']}s）=====")
    print(f"{'config':<20}{'client_ttft':>12}{'gw_ttft':>9}{'tps':>8}{'vram':>8}")
    for key, s in summary["configs"].items():
        print(f"{key:<20}{s['client_ttft_mean']:>12.3f}{s['gw_ttft_mean']:>9.3f}"
              f"{s['tps_mean']:>8.2f}{s['vram_max_mb']:>8}")
    print(f"明细 {args.out}\n汇总 {args.summary}")


if __name__ == "__main__":
    main()


