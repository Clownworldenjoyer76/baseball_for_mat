#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/splithome.py

Split Home Batters

Input:
  - Batters file (e.g., data/cleaned/batters_today.csv)
  - Games file   (e.g., data/raw/todaysgames_normalized.csv)

Output:
  - Home batters file (e.g., data/adjusted/batters_home.csv)

Guarantees:
  - Merge home-team batters with their game row
  - ID columns exported as integers (no decimals): team_id, home_team_id, away_team_id, opponent_team_id, game_id
"""

import sys
import pandas as pd

# --- helpers ---
def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace in all object columns, convert empty strings to <NA>."""
    obj_cols = df.select_dtypes(include=['object']).columns
    if len(obj_cols):
        df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip())
        # unify empties as NA so integer coercion won't upcast to float
        df[obj_cols] = df[obj_cols].replace({'': pd.NA})
    return df

def enforce_int(df: pd.DataFrame, cols) -> pd.DataFrame:
    """Coerce listed columns to pandas nullable integer (Int64) without decimals."""
    for col in cols:
        if col in df.columns:
            # to_numeric handles strings; errors='coerce' yields NA for bad values
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

# --- main ---
def main(batters_path: str, games_path: str, output_path: str) -> None:
    batters = pd.read_csv(batters_path, dtype=str, keep_default_na=False)
    games = pd.read_csv(games_path, dtype=str, keep_default_na=False)

    batters = strip_strings(batters)
    games = strip_strings(games)

    merged = batters.merge(
        games,
        left_on="team_id",
        right_on="home_team_id",
        how="inner",
        validate="m:1",
        suffixes=("", "_g"),
    )

    merged["side"] = "home"
    merged["opponent_team_id"] = merged["away_team_id"]

    id_cols = ["team_id", "home_team_id", "away_team_id", "opponent_team_id", "game_id"]
    merged = enforce_int(merged, id_cols)

    # Keep all original batter columns first, then the rest
    batter_cols = [c for c in batters.columns if c in merged.columns]
    extra_cols = [c for c in merged.columns if c not in batter_cols]
    merged = merged[batter_cols + extra_cols]

    # Export. Int64 dtype writes as integers (no decimals). Index excluded.
    merged.to_csv(output_path, index=False)

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 3:
        batters_file, games_file, output_file = args
    else:
        batters_file, games_file, output_file = (
            "data/cleaned/batters_today.csv",
            "data/raw/todaysgames_normalized.csv",
            "data/adjusted/batters_home.csv",
        )
    main(batters_file, games_file, output_file)
