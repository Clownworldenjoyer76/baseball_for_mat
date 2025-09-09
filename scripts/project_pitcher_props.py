#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
IN_1 = DATA_DIR / "_projections" / "todaysgames_normalized_fixed.csv"
IN_2 = DATA_DIR / "_projections" / "pitcher_mega_z.csv"
OUT  = DATA_DIR / "_projections" / "pitcher_props_projected.csv"

EXPECT_GAMES_COLS = ["home_team_id","away_team_id","pitcher_home_id","pitcher_away_id"]
EXPECT_MEGA_COLS  = ["player_id"]
MIN_ROWS_WARN = 1

def _require(df: pd.DataFrame, cols: list, name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise KeyError(f"{name} missing required columns: {miss}")

def main() -> int:
    if not IN_1.exists(): raise FileNotFoundError(f"Missing input: {IN_1}")
    if not IN_2.exists(): raise FileNotFoundError(f"Missing input: {IN_2}")

    games = pd.read_csv(IN_1)
    mega  = pd.read_csv(IN_2)
    games.columns = [c.strip() for c in games.columns]
    mega.columns  = [c.strip() for c in mega.columns]
    _require(games, EXPECT_GAMES_COLS, "todaysgames_normalized_fixed")
    _require(mega,  EXPECT_MEGA_COLS,  "pitcher_mega_z")

    # Example merge (minimal placeholder)
    left = games.copy()
    right = mega.add_suffix("_r")
    merged = left.merge(right, left_on="pitcher_home_id", right_on="player_id_r", how="left", validate="m:1")

    # Prepare output
    out_df = merged.copy()

    # === ENFORCE STRING IDS ===
    for __c in ["home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id", "player_id", "team_id", "game_id"]:
        if __c in out_df.columns:
            out_df[__c] = out_df[__c].astype("string")
    # === END ENFORCE ===
    out_df.to_csv(OUT, index=False)

    print(f"Wrote: {OUT} (rows={len(out_df)})  source=enriched")
    if len(out_df) < MIN_ROWS_WARN:
        print("WARNING: very few rows written")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
