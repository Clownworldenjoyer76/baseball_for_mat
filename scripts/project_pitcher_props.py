#!/usr/bin/env python3
# Purpose: Produce pitcher_props_projected.csv with game/team context
# Inputs:
#   - data/_projections/pitcher_props_projected.csv  (from enriched source)
#   - data/raw/startingpitchers_with_opp_context.csv (from project_prep.py long format)
# Output:
#   - data/_projections/pitcher_props_projected.csv  (overwrites with context columns added)

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
PROJ_IN  = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SP_LONG  = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
PROJ_OUT = PROJ_IN  # overwrite in place

VERSION = "v4-context-from-sp_long"

REQ_PROJ_COLS = ["player_id"]           # minimal needed to merge
REQ_SP_COLS   = ["game_id","team_id","opponent_team_id","player_id"]
CTX_COLS      = ["game_id","team_id","opponent_team_id"]

def log(msg: str) -> None:
    print(msg, flush=True)

def must_have(df: pd.DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing required column(s): {missing}")

def main() -> int:
    log(f">> START: project_pitcher_props.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_pitcher_props] VERSION={VERSION} @ {Path(__file__).resolve()}")

    if not PROJ_IN.exists():
        raise FileNotFoundError(f"Missing input: {PROJ_IN}")
    if not SP_LONG.exists():
        raise FileNotFoundError(f"Missing input: {SP_LONG}")

    # Load inputs as strings and force-fill
    proj = pd.read_csv(PROJ_IN, dtype=str).fillna("UNKNOWN")
    sp   = pd.read_csv(SP_LONG, dtype=str).fillna("UNKNOWN")

    must_have(proj, REQ_PROJ_COLS, str(PROJ_IN))
    must_have(sp,   REQ_SP_COLS,   str(SP_LONG))

    # Keep only needed context columns from sp_long
    sp_ctx = sp[REQ_SP_COLS].drop_duplicates()

    # Merge: add game_id/team_id/opponent_team_id onto projected rows
    merged = proj.merge(sp_ctx, on="player_id", how="left", suffixes=("", "_sp"))

    # If any context columns are missing after merge, fill with "UNKNOWN"
    for c in CTX_COLS:
        if c not in merged.columns:
            merged[c] = "UNKNOWN"
        merged[c] = merged[c].fillna("UNKNOWN").astype(str)

    # Final: ensure ALL columns are strings, no NaN
    for c in merged.columns:
        merged[c] = merged[c].astype(str).fillna("UNKNOWN")

    # Write back to same output path (downstream expects this file)
    PROJ_OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(PROJ_OUT, index=False)

    log(f"Wrote: {PROJ_OUT} (rows={len(merged)})  source=props+sp_long_context")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(e)
        sys.exit(1)
