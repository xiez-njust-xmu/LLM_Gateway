import json
import os
import datetime

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
# 多实例部署时可让每个实例写各自的文件（GATEWAY_LOG_FILE），默认仍是 logs/gateway.log
LOG_FILE = os.getenv("GATEWAY_LOG_FILE") or os.path.join(LOG_DIR, "gateway.log")

os.makedirs(os.path.dirname(LOG_FILE) or LOG_DIR, exist_ok=True)


def log_request(record: dict):
    record["time"] = datetime.datetime.now().isoformat()
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")