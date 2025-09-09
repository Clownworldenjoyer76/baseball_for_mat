#!/usr/bin/env python3
# Fail the run if any scheduled starter is missing in pitcher_mega_z,
# but ALWAYS write coverage/missing reports first.

import pandas as pd
from pathlib import Path
import sys

GAMES_FILE = Path("data/_projections/todaysgames_normalized_fixed.csv")
MEGA_FILE  = Path("data/_projections/pitcher_mega_z.csv")
OUT_DIR    = Path("summaries/projections")
COVERAGE_CSV = OUT_DIR / "mega_z_starter_coverage.csv"
MISSING_CSV  = OUT_DIR / "mega_z_starter_missing.csv"

def main():
    if not GAMES_FILE.exists():
        raise SystemExit(f"Missing schedule: {GAMES_FILE}")
    if not MEGA_FILE.exists():
        raise SystemExit(f"Missing mega_z file: {MEGA_FILE}")

    games = pd.read_csv(GAMES_FILE, dtype=str)
    mega  = pd.read_csv(MEGA_FILE, dtype=str)

    games_cols_req = {"pitcher_home_id","pitcher_away_id"}
    if not games_cols_req.issubset(set(games.columns)):
        raise SystemExit(f"Schedule missing required columns: {games_cols_req - set(games.columns)}")

    mega_ids = set(mega["player_id"].astype(str)) if "player_id" in mega.columns else set()

    starters = pd.unique(pd.concat([games["pitcher_home_id"], games["pitcher_away_id"]]).astype(str))
    df_cov = pd.DataFrame({"player_id": starters})
    df_cov["present_in_mega_z"] = df_cov["player_id"].isin(mega_ids)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df_cov.to_csv(COVERAGE_CSV, index=False)

    missing = df_cov[~df_cov["present_in_mega_z"]].copy()
    missing.to_csv(MISSING_CSV, index=False)

    missing_count = int(missing.shape[0])
    if missing_count > 0:
        raise RuntimeError(f"Starter coverage failure: {missing_count} starter(s) absent in pitcher_mega_z. "
                           f"See {COVERAGE_CSV} and {MISSING_CSV}.")

    print("starter_coverage_guard: OK (all starters present)")

if __name__ == "__main__":
    main()
