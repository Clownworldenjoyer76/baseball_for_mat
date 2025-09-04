#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
/scripts/build_batters_today_from_lineups.py

Simplified builder for batters_today.csv.

Instead of re-merging on names, this script trusts the normalized lineups file
(data/raw/lineups_normalized.csv), which already has player_id, type, and team_id.

Steps:
  1. Load data/raw/lineups_normalized.csv
  2. Keep only required columns
  3. Deduplicate (by player_id if available, else by team_code + name)
  4. Save to data/cleaned/batters_today.csv
"""

from pathlib import Path
import pandas as pd

LINEUPS = Path("data/raw/lineups_normalized.csv")
OUTFILE = Path("data/cleaned/batters_today.csv")

REQ_COLS = ["team_code", "last_name, first_name", "type", "player_id", "team_id"]

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

    df = pd.read_csv(LINEUPS, dtype=str, keep_default_na=False)

    # Ensure all required columns exist
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{LINEUPS} is missing required columns: {missing}")

    df = df[REQ_COLS]

    # Deduplicate
    if df["player_id"].notna().any() and (df["player_id"] != "").any():
        df = df.drop_duplicates(subset=["player_id"], keep="first")
    else:
        df = df.drop_duplicates(subset=["team_code", "last_name, first_name"], keep="first")

    df = enforce_types(df)

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTFILE, index=False)

    print(f"✅ build_batters_today_from_lineups: wrote {len(df)} rows -> {OUTFILE}")

if __name__ == "__main__":
    main()
