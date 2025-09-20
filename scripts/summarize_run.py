#!/usr/bin/env python3
"""
Summarize the daily projection run.

Behavior:
- Prints row counts for all final outputs.
- If gamescores_bad_games.txt exists, logs a WARNING but exits 0.
- Exits 1 only if a required output file is missing or empty.

Required final outputs:
  data/end_chain/final/batter_event_probabilities.csv
  data/end_chain/final/pitcher_event_probabilities.csv
  data/end_chain/final/game_score_projections.csv
"""

from __future__ import annotations

import sys
import ast
import pandas as pd
from pathlib import Path

SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")

BATTER_EVENTS = OUT_DIR / "batter_event_probabilities.csv"
PITCHER_EVENTS = OUT_DIR / "pitcher_event_probabilities.csv"
GAME_SCORES   = OUT_DIR / "game_score_projections.csv"

BAD_GAMES_TXT = SUM_DIR / "gamescores_bad_games.txt"
BAD_GAMES_DETAIL = SUM_DIR / "gamescores_bad_games_detail.csv"

def rows_of(p: Path) -> int:
    if not p.exists():
        return -1
    try:
        return len(pd.read_csv(p))
    except Exception:
        return -1

def parse_bad_games(p: Path) -> dict[int, int]:
    """Parse a line like: 'bad_games={776271: 1, 776272: 1}'"""
    try:
        txt = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    # Find the first '{...}' on the line
    start = txt.find("{")
    end   = txt.rfind("}")
    if start == -1 or end == -1 or end < start:
        return {}
    try:
        return {int(k): int(v) for k, v in ast.literal_eval(txt[start:end+1]).items()}
    except Exception:
        return {}

def main() -> int:
    # Summarize final outputs
    batter_rows  = rows_of(BATTER_EVENTS)
    pitcher_rows = rows_of(PITCHER_EVENTS)
    games_rows   = rows_of(GAME_SCORES)

    print("=== Final Outputs ===")
    print(f"{BATTER_EVENTS}: rows={batter_rows}")
    print(f"{PITCHER_EVENTS}: rows={pitcher_rows}")
    print(f"{GAME_SCORES}: rows={games_rows}")

    # Treat missing/empty outputs as fatal
    fatal = []
    if batter_rows <= 0:  fatal.append(str(BATTER_EVENTS))
    if pitcher_rows <= 0: fatal.append(str(PITCHER_EVENTS))
    if games_rows <= 0:   fatal.append(str(GAME_SCORES))

    # Bad games are warnings only
    bad = parse_bad_games(BAD_GAMES_TXT)
    if bad:
        print("\n=== WARNING: Incomplete/duplicated games were dropped ===")
        print(f"{BAD_GAMES_TXT}: bad_games={bad}")
        if BAD_GAMES_DETAIL.exists():
            print(f"{BAD_GAMES_DETAIL}: details written")

    # Exit code
    if fatal:
        print("\nERROR: Required outputs missing or empty:")
        for f in fatal:
            print(f" - {f}")
        return 1

    print("\nOK: summarize_run completed (warnings above are non-fatal).")
    return 0

if __name__ == "__main__":
    sys.exit(main())
