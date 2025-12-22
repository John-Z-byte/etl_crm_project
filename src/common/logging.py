# src/common/logging.py
from datetime import datetime

def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def info(msg: str): print(f"{_ts()} | INFO  | {msg}")
def warn(msg: str): print(f"{_ts()} | WARN  | {msg}")
def ok(msg: str):   print(f"{_ts()} | OK    | {msg}")
