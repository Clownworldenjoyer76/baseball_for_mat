#!/usr/bin/env python3
# scripts/project_prep.py
# Purpose: build startingpitchers.csv and startingpitchers_with_opp_context.csv
# Ensures startingpitchers_with_opp_context.csv has: game_id, team_id, opponent_team_id, player_id (all as strings, no NaN)

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---- Paths ----
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
END_DIR = ROOT / "data" / "end_chain" / "final"

STARTING_PITCHERS_OUT = END_DIR / "startingpitchers.csv"
WITH_OPP_OUT = RAW_DIR / "startingpitchers_with_opp_context.csv"

VERSION = "v3-fixed"

def log(msg: str) -> None:
    print(msg, flush=True)

def main() -> int:
    log(f">> START: project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_prep] VERSION={VERSION} @ {Path(__file__).resolve()}")

    # -------------------------------------------------------------------------
    # INPUT: try to read existing startingpitchers.csv if present
    # -------------------------------------------------------------------------
    sp_path_guess = STARTING_PITCHERS_OUT
    if sp_path_guess.exists():
        sp = pd.read_csv(sp_path_guess, dtype=str)
    else:
        raise RuntimeError(
            "project_prep.py expected to build `sp` earlier in the pipeline.\n"
            "Ensure `sp` exists with columns: game_id, home_team_id, away_team_id, "
            "pitcher_home_id, pitcher_away_id (all as strings)."
        )

    # Normalize expected ID columns to strings
    for col in ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]:
        if col in sp.columns:
            sp[col] = sp[col].astype(str)

    # -------------------------------------------------------------------------
    # OUTPUT 1: startingpitchers.csv (unchanged except dtype normalization)
    # -------------------------------------------------------------------------
    STARTING_PITCHERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    sp.to_csv(STARTING_PITCHERS_OUT, index=False)
    log(f"project_prep: wrote {STARTING_PITCHERS_OUT} (rows={len(sp)})")

    # -------------------------------------------------------------------------
    # OUTPUT 2: startingpitchers_with_opp_context.csv
    # Must include ONLY: game_id, team_id, opponent_team_id, player_id
    # -------------------------------------------------------------------------
    required_source = {"game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"}
    missing = [c for c in required_source if c not in sp.columns]
    if missing:
        raise RuntimeError(
            f"project_prep: missing required columns in `sp` for with_opp_context: {missing}"
        )

    # Home rows
    home_rows = pd.DataFrame({
        "game_id": sp["game_id"],
        "team_id": sp["home_team_id"],
        "opponent_team_id": sp["away_team_id"],
        "player_id": sp["pitcher_home_id"],
    })

    # Away rows
    away_rows = pd.DataFrame({
        "game_id": sp["game_id"],
        "team_id": sp["away_team_id"],
        "opponent_team_id": sp["home_team_id"],
        "player_id": sp["pitcher_away_id"],
    })

    # Combine + enforce no NaN, all strings
    with_opp = pd.concat([home_rows, away_rows], ignore_index=True)
    with_opp = with_opp.fillna("").astype(str)

    # Final column order
    with_opp = with_opp[["game_id", "team_id", "opponent_team_id", "player_id"]]

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
