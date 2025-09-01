#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Split Home Batters

Input:
  - Batters file (e.g., data/temp_inputs/batters_today_copy.csv)
  - Games file   (e.g., data/temp_inputs/todaysgames_normalized_copy.csv)

Output:
  - Home batters file (e.g., data/adjusted/batters_home.csv)

Ensures:
  - Joins only home-team batters to their games
  - All ID columns (team_id, home_team_id, away_team_id, opponent_team_id, game_id)
    are exported as clean integers, no decimals
"""

import sys
import pandas as pd

def enforce_int(df: pd.DataFrame, cols) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

def main(batters_path, games_path, output_path):
    batters = pd.read_csv(batters_path, dtype=str, keep_default_na=False)
    games = pd.read_csv(games_path, dtype=str, keep_default_na=False)

    merged = batters.merge(
        games,
        left_on="team_id",
        right_on="home_team_id",
        how="inner",
        validate="m:1",
        suffixes=("", "_g")
    )

    merged["side"] = "home"
    merged["opponent_team_id"] = merged["away_team_id"]

    id_cols = ["team_id", "home_team_id", "away_team_id", "opponent_team_id", "game_id"]
    merged = enforce_int(merged, id_cols)

    batter_cols = [c for c in batters.columns if c in merged.columns]
    extra_cols = [c for c in merged.columns if c not in batter_cols]
    merged = merged[batter_cols + extra_cols]

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
