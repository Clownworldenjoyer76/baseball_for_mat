# scripts/logging_utils.py
import sys

def log(message: str):
    sys.stdout.write(f"[BET_TRACKER] {message}\n")
    sys.stdout.flush()
