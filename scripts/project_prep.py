#!/usr/bin/env python3
# scripts/project_prep.py
# Purpose: build startingpitchers.csv and startingpitchers_with_opp_context.csv
# NOTE: No new files or paths are introduced. Only ensures required columns exist.

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---- Paths (unchanged destinations) ----
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
END_DIR = ROOT / "data" / "end_chain" / "final"

STARTING_PITCHERS_OUT = END_DIR / "startingpitchers.csv"
WITH_OPP_OUT = RAW_DIR / "startingpitchers_with_opp_context.csv"

VERSION = "v3-forcedfill"

def log(msg: str) -> None:
    print(msg, flush=True)

def main() -> int:
    log(f">> START: project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_prep] VERSION={VERSION} @ {Path(__file__).resolve()}")

    # -------------------------------------------------------------------------
    # BEGIN: INPUT ASSEMBLY (expects `sp` from prior steps or fallback)
    # -------------------------------------------------------------------------
    sp_path_guess = END_DIR / "startingpitchers.csv"
    if sp_path_guess.exists():
        sp = pd.read_csv(sp_path_guess, dtype=str)
    else:
        raise RuntimeError(
            "project_prep.py expected to build `sp` earlier in this script.\n"
            "Ensure `sp` exists with columns: game_id, home_team_id, away_team_id, "
            "pitcher_home_id, pitcher_away_id (all as strings)."
        )

    # Ensure key id columns are strings
    for col in ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]:
        if col in sp.columns:
            sp[col] = sp[col].astype(str)

    # -------------------------------------------------------------------------
    # BUILD OUTPUTS
    # -------------------------------------------------------------------------
    # 1) startingpitchers.csv
    STARTING_PITCHERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    sp.to_csv(STARTING_PITCHERS_OUT, index=False)
    log(f"project_prep: wrote {STARTING_PITCHERS_OUT} (rows={len(sp)})")

    # 2) startingpitchers_with_opp_context.csv
    required_source = {"game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"}
    missing = [c for c in required_source if c not in sp.columns]
    if missing:
        raise RuntimeError(
            f"project_prep: missing required columns in `sp` for with_opp_context: {missing}"
        )

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

    with_opp = pd.concat([home_rows, away_rows], ignore_index=True)

    # Guarantee string dtypes and replace NaN with "UNKNOWN"
    for col in ["game_id", "team_id", "opponent_team_id", "player_id"]:
        with_opp[col] = with_opp[col].astype(str).fillna("UNKNOWN").replace("nan", "UNKNOWN")

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
