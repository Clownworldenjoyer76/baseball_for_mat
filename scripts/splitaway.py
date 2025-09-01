#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/splitaway.py

Split away batters and ensure ID columns export as plain integers.
"""

import sys
import pandas as pd

ID_COLS = [
    "team_id",
    "home_team_id",
    "away_team_id",
    "opponent_team_id",
    "game_id",
]

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    obj = df.select_dtypes(include=["object"]).columns
    if len(obj):
        df[obj] = df[obj].apply(lambda s: s.str.strip())
        df[obj] = df[obj].replace({"": pd.NA})
    return df

def enforce_int64(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def render_int_cols_for_csv(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64").astype("string")
            df[c] = df[c].replace({"<NA>": ""})
    return df

def main(batters_path: str, games_path: str, output_path: str) -> None:
    batters = pd.read_csv(batters_path, dtype=str, keep_default_na=False)
    games = pd.read_csv(games_path, dtype=str, keep_default_na=False)

    batters = strip_strings(batters)
    games = strip_strings(games)

    merged = batters.merge(
        games,
        left_on="team_id",
        right_on="away_team_id",
        how="inner",
        validate="m:1",
        suffixes=("", "_g"),
    )

    merged["side"] = "away"
    merged["opponent_team_id"] = merged["home_team_id"]

    merged = enforce_int64(merged, ID_COLS)

    batter_cols = [c for c in batters.columns if c in merged.columns]
    extra_cols = [c for c in merged.columns if c not in batter_cols]
    merged = merged[batter_cols + extra_cols]

    merged = render_int_cols_for_csv(merged, ID_COLS)

    merged.to_csv(output_path, index=False)

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 3:
        batters_file, games_file, output_file = args
    else:
        batters_file, games_file, output_file = (
            "data/cleaned/batters_today.csv",
            "data/raw/todaysgames_normalized.csv",
            "data/adjusted/batters_away.csv",
        )
    main(batters_file, games_file, output_file)
