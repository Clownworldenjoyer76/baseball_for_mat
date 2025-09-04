#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
/scripts/build_batters_today_from_lineups.py

Builds data/cleaned/batters_today.csv using enriched stats.

Inputs:
  - data/raw/lineups_normalized.csv
  - data/Data/batters.csv (includes woba, xwoba, etc.)

Output:
  - data/cleaned/batters_today.csv with:
    team_code, last_name, first_name, type, player_id, team_id, woba, xwoba
"""

from pathlib import Path
import pandas as pd

LINEUPS = Path("data/raw/lineups_normalized.csv")
BATTERS = Path("data/Data/batters.csv")
OUTFILE = Path("data/cleaned/batters_today.csv")

REQ_LINEUP_COLS = ["team_code", "last_name, first_name", "type", "player_id", "team_id"]
BATTER_STATS = ["woba", "xwoba"]  # required downstream

def enforce_types(df: pd.DataFrame) -> pd.DataFrame:
    if "team_id" in df.columns:
        df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64").astype("string")
        df["team_id"] = df["team_id"].replace({"<NA>": ""})
    if "player_id" in df.columns:
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64").astype("string")
        df["player_id"] = df["player_id"].replace({"<NA>": ""})
    if "type" in df.columns:
        df["type"] = df["type"].fillna("").astype(str)
    return df

def main():
    if not LINEUPS.exists():
        raise FileNotFoundError(f"{LINEUPS} not found")
    if not BATTERS.exists():
        raise FileNotFoundError(f"{BATTERS} not found")

    lu = pd.read_csv(LINEUPS, dtype=str, keep_default_na=False)
    bat = pd.read_csv(BATTERS, low_memory=False)

    # Ensure required columns
    missing = [c for c in REQ_LINEUP_COLS if c not in lu.columns]
    if missing:
        raise ValueError(f"{LINEUPS} is missing required columns: {missing}")
    for c in ["last_name, first_name", "player_id"]:
        if c not in bat.columns:
            raise ValueError(f"{BATTERS} is missing column: {c}")

    # Keep required lineup columns
    lu = lu[REQ_LINEUP_COLS].dropna(subset=["player_id"]).drop_duplicates()

    # Keep only player_id + stats from batters.csv
    bat_stats = bat[["player_id"] + [c for c in BATTER_STATS if c in bat.columns]].copy()

    # Ensure player_id is string for join
    lu["player_id"] = pd.to_numeric(lu["player_id"], errors="coerce").astype("Int64").astype("string")
    bat_stats["player_id"] = pd.to_numeric(bat_stats["player_id"], errors="coerce").astype("Int64").astype("string")

    # Merge stats into lineups
    out = lu.merge(bat_stats, on="player_id", how="left")

    # Deduplicate by player_id
    out = out.drop_duplicates(subset=["player_id"], keep="first")

    # Types
    out = enforce_types(out)

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTFILE, index=False)

    print(f"✅ build_batters_today_from_lineups: wrote {len(out)} rows -> {OUTFILE}")

if __name__ == "__main__":
    main()
