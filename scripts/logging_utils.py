# scripts/logging_utils.py
import sys
def log(msg: str):
    sys.stdout.write(f"[BET_TRACKER] {msg}\n")
    sys.stdout.flush()
