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

# If your script reads from existing normalized inputs, keep those reads as-is.
# I’m not inventing any inputs here. This script expects that the upstream step(s)
# already produced the dataframe `sp` below. If you currently read from files,
# keep those reads. For clarity, the transformation section starts at "BUILD OUTPUTS".

VERSION = "v3"

def log(msg: str) -> None:
    print(msg, flush=True)

def main() -> int:
    log(f">> START: project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_prep] VERSION={VERSION} @ {Path(__file__).resolve()}")

    # -------------------------------------------------------------------------
    # BEGIN: YOUR EXISTING INPUT ASSEMBLY
    # -------------------------------------------------------------------------
    # Replace the block below with your current input logic as-is.
    # The only thing that matters for the fix is that we end up with a dataframe
    # named `sp` that has at least these columns:
    #   game_id, home_team_id, away_team_id, pitcher_home_id, pitcher_away_id
    #
    # If you already have that earlier in the file, keep it. I'm keeping this
    # placeholder minimal and neutral.

    # Example: if you already have 'sp' built earlier, just comment this out.
    # Here we try to load the file you showed in messages, but ONLY if it exists.
    sp_path_guess = END_DIR / "startingpitchers.csv"
    if sp_path_guess.exists():
        sp = pd.read_csv(sp_path_guess, dtype=str)
    else:
        # If upstream in this same script normally builds `sp`, you should
        # remove these two lines and keep your original build. We fail loudly
        # so we don't silently change behavior.
        raise RuntimeError(
            "project_prep.py expected to build `sp` earlier in this script.\n"
            "Ensure `sp` exists with columns: game_id, home_team_id, away_team_id, "
            "pitcher_home_id, pitcher_away_id (all as strings)."
        )

    # Ensure key id columns are strings (dtype normalization only—no value changes)
    for col in ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]:
        if col in sp.columns:
            sp[col] = sp[col].astype(str)

    # -------------------------------------------------------------------------
    # BUILD OUTPUTS (this is the only section that changes behavior):
    # 1) Write startingpitchers.csv (unchanged shape/columns from your build)
    # 2) Build startingpitchers_with_opp_context.csv with required columns
    # -------------------------------------------------------------------------

    # 1) startingpitchers.csv — write exactly what you already had
    STARTING_PITCHERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    sp.to_csv(STARTING_PITCHERS_OUT, index=False)
    log("project_prep: wrote data/end_chain/final/startingpitchers.csv "
        f"(rows={len(sp)})")

    # 2) startingpitchers_with_opp_context.csv — ensure required columns
    # We produce two rows per game: one for home starter, one for away starter.
    required_source = {"game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"}
    missing = [c for c in required_source if c not in sp.columns]
    if missing:
        raise RuntimeError(
            f"project_prep: missing required columns in `sp` for with_opp_context: {missing}"
        )

    # Build per-pitcher rows (NO extra columns required by downstream beyond these)
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

    # Guarantee string dtypes (defensive; keeps everything uniform)
    for col in ["game_id", "team_id", "opponent_team_id", "player_id"]:
        with_opp[col] = with_opp[col].astype(str)

    # Final minimal column order that other scripts rely on
    with_opp = with_opp[["game_id", "team_id", "opponent_team_id", "player_id"]]

    WITH_OPP_OUT.parent.mkdir(parents=True, exist_ok=True)
    with_opp.to_csv(WITH_OPP_OUT, index=False)

    log("project_prep: wrote data/raw/startingpitchers_with_opp_context.csv "
        f"(rows={len(with_opp)})")
    log("[END] project_prep.py "
        f"({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        # Match your logging style without inventing files
        print(str(e))
        sys.exit(1)
