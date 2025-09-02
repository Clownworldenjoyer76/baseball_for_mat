#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import sys

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
OUT_FILE = GAMES_FILE  # in-place update

REQUIRED_GAMES = {"game_id", "pitcher_home", "pitcher_away"}
REQUIRED_MASTER = {"name", "player_id"}

def main():
    # Load
    games = pd.read_csv(GAMES_FILE)
    master = pd.read_csv(MASTER_FILE)

    # Validate schemas
    miss_g = REQUIRED_GAMES - set(games.columns)
    if miss_g:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(miss_g)}")
    miss_m = REQUIRED_MASTER - set(master.columns)
    if miss_m:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {MASTER_FILE} missing columns: {sorted(miss_m)}")

    # Normalize types
    games["game_id"] = pd.to_numeric(games["game_id"], errors="coerce").astype("Int64")
    for c in ["pitcher_home", "pitcher_away"]:
        games[c] = games[c].astype(str).str.strip()

    master["name"] = master["name"].astype(str).str.strip()
    master["player_id"] = pd.to_numeric(master["player_id"], errors="coerce").astype("Int64")

    # Build mapping: name -> player_id
    # If duplicates exist, keep first occurrence deterministically
    name_to_id = (master.dropna(subset=["name", "player_id"])
                        .drop_duplicates(subset=["name"])
                        .set_index("name")["player_id"])

    # Map to IDs
    games["pitcher_home_id"] = games["pitcher_home"].map(name_to_id).astype("Int64")
    games["pitcher_away_id"] = games["pitcher_away"].map(name_to_id).astype("Int64")

    # Write out
    games.to_csv(OUT_FILE, index=False)
    print(f"✅ Injected pitcher IDs -> {OUT_FILE} "
          f"(home_id non-null: {games['pitcher_home_id'].notna().sum()}, "
          f"away_id non-null: {games['pitcher_away_id'].notna().sum()})")

if __name__ == "__main__":
    sys.exit(main())
