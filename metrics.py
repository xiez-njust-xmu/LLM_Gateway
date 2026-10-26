import time
import subprocess

def get_gpu_stats():
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used",
             "--format=csv,noheader,nounits"]
        ).decode().strip()
        util, mem = out.split(", ")
        return int(util), int(mem)
    except Exception:
        return 0, 0

def compute_metrics(response: dict):
    """从 Ollama 响应中提取指标"""
    ttft = (response.get("load_duration", 0) + response.get("prompt_eval_duration", 0)) / 1e9
    tps = response.get("eval_count", 0) / (response.get("eval_duration", 0) / 1e9) if response.get("eval_duration") else 0
    return {
        "ttft": round(ttft, 3),
        "tps": round(tps, 2),
        "prompt_tokens": response.get("prompt_eval_count", 0),
        "completion_tokens": response.get("eval_count", 0),
    }