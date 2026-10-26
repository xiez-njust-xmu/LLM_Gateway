# load_test.py
import requests, time, concurrent.futures

URL = "http://localhost:8000/v1/chat/completions"
HEADERS = {"X-API-Key": "sk-prod-001", "Content-Type": "application/json"}

def call(i):
    payload = {
        "model": "qwen0.5b",
        "messages": [{"role": "user", "content": f"第 {i} 个问题：什么是 paged attention"}],
    }
    t0 = time.time()
    r = requests.post(URL, json=payload, headers=HEADERS)
    t1 = time.time()
    return t1 - t0

for conc in [1, 2, 4]:
    print(f"\n=== concurrency={conc} ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=conc) as ex:
        latencies = list(ex.map(call, range(conc)))
    print("avg:", round(sum(latencies)/len(latencies), 2), "s")
    print("p95:", round(sorted(latencies)[int(len(latencies)*0.95)-1], 2), "s")