# scripts/ci_runner.py
"""Lightweight CI wrapper so YAML stays minimal.
- Prints pre/post line counts for history CSVs
- Runs the tracker and streams logs to run.log
- Exits with the tracker's exit code
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1] if (Path(__file__).resolve().parents[1] / "scripts").exists() else Path.cwd()
SCRIPTS = ROOT / "scripts"
DATA_PLAYERS = ROOT / "data" / "bets" / "player_props_history.csv"
DATA_GAMES = ROOT / "data" / "bets" / "game_props_history.csv"
LOG_FILE = ROOT / "run.log"

def line_count(p: Path) -> int:
    if not p.exists():
        return 0
    try:
        with p.open("rb") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0

def snapshot(tag: str):
    print(f"===== {tag} SNAPSHOT @ {datetime.utcnow().isoformat()}Z =====")
    for p in (DATA_PLAYERS, DATA_GAMES):
        exists = p.exists()
        count = line_count(p)
        rel = str(p.relative_to(ROOT)) if p.exists() or p.is_absolute() else str(p)
        print(f"{rel} -> {'MISSING' if not exists else f'{count} lines'}")

def run_tracker() -> int:
    # Prefer modular main.py; fallback to legacy bet_tracker.py
    main_py = SCRIPTS / "main.py"
    legacy_py = SCRIPTS / "bet_tracker.py"
    target = main_py if main_py.exists() else legacy_py
    if not target.exists():
        print(f"ERROR: No tracker entrypoint found at {target} or {legacy_py}", file=sys.stderr)
        return 2

    cmd = [sys.executable, "-u", "-X", "dev", str(target)]
    print(f"Running: {' '.join(cmd)}")
    LOG_FILE.write_text("")  # truncate
    with LOG_FILE.open("ab") as logf:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(ROOT))
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.buffer.write(line)
            sys.stdout.flush()
            logf.write(line)
            logf.flush()
        proc.wait()
        return proc.returncode

def main():
    print(f"Python: {sys.version.split()[0]}  CWD: {Path.cwd()}")
    snapshot("PRE")
    code = run_tracker()
    snapshot("POST")
    print(f"Exit code: {code}")
    sys.exit(code)

if __name__ == "__main__":
    main()
