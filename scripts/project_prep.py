#!/usr/bin/env python3
# Purpose: build startingpitchers_with_opp_context.csv in the long shape
#          and (as before) write startingpitchers.csv to end_chain/final.
# No new inputs/outputs/paths.

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
END_DIR = ROOT / "data" / "end_chain" / "final"

STARTING_PITCHERS_OUT = END_DIR / "startingpitchers.csv"
WITH_OPP_OUT = RAW_DIR / "startingpitchers_with_opp_context.csv"

VERSION = "v3-forcedfill"

REQ_LONG = ["game_id", "team_id", "opponent_team_id", "player_id"]
REQ_WIDE = ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]

def log(msg: str) -> None:
    print(msg, flush=True)

def _as_str(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)

def _fill_unknown(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].fillna("UNKNOWN").replace({"nan": "UNKNOWN", "NaN": "UNKNOWN", "None": "UNKNOWN"})

def main() -> int:
    log(f">> START: project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_prep] VERSION={VERSION} @ {Path(__file__).resolve()}")

    # Expect upstream to have produced (or this script earlier produced) startingpitchers.csv
    if not STARTING_PITCHERS_OUT.exists():
        raise RuntimeError(f"Missing required input file: {STARTING_PITCHERS_OUT}")

    sp = pd.read_csv(STARTING_PITCHERS_OUT, dtype=str)

    # Always persist startingpitchers.csv exactly as-is (idempotent)
    STARTING_PITCHERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    sp.to_csv(STARTING_PITCHERS_OUT, index=False)
    log(f"project_prep: wrote {STARTING_PITCHERS_OUT} (rows={len(sp)})")

    cols = [c.strip() for c in sp.columns]
    has_long = all(c in cols for c in REQ_LONG)
    has_wide = all(c in cols for c in REQ_WIDE)

    if has_long:
        # Normalize and write directly
        _as_str(sp, REQ_LONG)
        _fill_unknown(sp, REQ_LONG)
        out = sp[REQ_LONG].copy()
    elif has_wide:
        # Convert wide -> long
        _as_str(sp, REQ_WIDE)
        _fill_unknown(sp, REQ_WIDE)

        home_rows = pd.DataFrame({
            "game_id": sp["game_id"],
            "team_id": sp["home_team_id"],
            "opponent_team_id": sp["away_team_id"],
            "player_id": sp["pitcher_home_id"],
        })
        away_rows = pd.DataFrame({
            "game_id": sp["game_id"],
            "team_id": sp["away_team_id"],
            "opponent_team_id": sp["home_team_id"],
            "player_id": sp["pitcher_away_id"],
        })
        out = pd.concat([home_rows, away_rows], ignore_index=True)
        _as_str(out, REQ_LONG)
        _fill_unknown(out, REQ_LONG)
        out = out[REQ_LONG]
    else:
        # Tell exactly what's missing to stop the churn.
        missing_long = [c for c in REQ_LONG if c not in cols]
        missing_wide = [c for c in REQ_WIDE if c not in cols]
        raise RuntimeError(
            "project_prep: input schema not recognized.\n"
            f"Columns present: {cols}\n"
            f"To proceed, `startingpitchers.csv` must be either LONG {REQ_LONG} (preferred) "
            f"or WIDE {REQ_WIDE}."
        )

    WITH_OPP_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(WITH_OPP_OUT, index=False)
    log(f"project_prep: wrote {WITH_OPP_OUT} (rows={len(out)})")
    log(f"[END] project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(1)
