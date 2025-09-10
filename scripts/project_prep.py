#!/usr/bin/env python3
# scripts/project_prep.py
# Purpose: build startingpitchers.csv (wide) and startingpitchers_with_opp_context.csv (long)
# Source: data/raw/todaysgames_normalized.csv
# All IDs forced to strings, NaN replaced with "UNKNOWN"

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---- Paths ----
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
END_DIR = ROOT / "data" / "end_chain" / "final"

TODAY_GAMES = RAW_DIR / "todaysgames_normalized.csv"
STARTING_PITCHERS_OUT = END_DIR / "startingpitchers.csv"
WITH_OPP_OUT = RAW_DIR / "startingpitchers_with_opp_context.csv"

VERSION = "v5-tgn-clean"

def log(msg: str) -> None:
    print(msg, flush=True)

def main() -> int:
    log(f">> START: project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_prep] VERSION={VERSION} @ {Path(__file__).resolve()}")

    if not TODAY_GAMES.exists():
        raise FileNotFoundError(f"Missing required input: {TODAY_GAMES}")

    # Load todaysgames_normalized
    tg = pd.read_csv(TODAY_GAMES, dtype=str).fillna("UNKNOWN")

    # --- Wide format (one row per game) ---
    sp_wide = tg[[
        "game_id",
        "home_team_id",
        "away_team_id",
        "pitcher_home_id",
        "pitcher_away_id"
    ]].copy()

    for c in sp_wide.columns:
        sp_wide[c] = sp_wide[c].astype(str).fillna("UNKNOWN")

    STARTING_PITCHERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    sp_wide.to_csv(STARTING_PITCHERS_OUT, index=False)
    log(f"project_prep: wrote {STARTING_PITCHERS_OUT} (rows={len(sp_wide)})")

    # --- Long format (two rows per game: home + away pitcher) ---
    home_rows = pd.DataFrame({
        "game_id": tg["game_id"],
        "team_id": tg["home_team_id"],
        "opponent_team_id": tg["away_team_id"],
        "player_id": tg["pitcher_home_id"]
    })

    away_rows = pd.DataFrame({
        "game_id": tg["game_id"],
        "team_id": tg["away_team_id"],
        "opponent_team_id": tg["home_team_id"],
        "player_id": tg["pitcher_away_id"]
    })

    with_opp = pd.concat([home_rows, away_rows], ignore_index=True).fillna("UNKNOWN")
    for c in with_opp.columns:
        with_opp[c] = with_opp[c].astype(str)

    WITH_OPP_OUT.parent.mkdir(parents=True, exist_ok=True)
    with_opp.to_csv(WITH_OPP_OUT, index=False)
    log(f"project_prep: wrote {WITH_OPP_OUT} (rows={len(with_opp)})")

    log(f"[END] project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(1)
