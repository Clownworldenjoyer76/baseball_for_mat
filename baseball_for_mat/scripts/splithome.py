#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/splithome.py

Split home batters and ensure ID columns export as plain integers.
"""

import sys
import pandas as pd
from pathlib import Path

ID_COLS = [
    "team_id",
    "home_team_id",
    "away_team_id",
    "opponent_team_id",
    "game_id",
]

TEAM_DIR = Path("data/manual/team_directory.csv")  # optional fallback mapping

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
            df[c] = df[c].astype("Int64").astype("string").replace({"<NA>": ""})
    return df

def fill_team_id_from_directory(batters: pd.DataFrame) -> pd.DataFrame:
    if "team_id" in batters.columns and batters["team_id"].notna().any():
        return batters
    if "team_code" not in batters.columns or not TEAM_DIR.exists():
        return batters
    dir_df = pd.read_csv(TEAM_DIR, dtype=str, keep_default_na=False)
    dir_df = strip_strings(dir_df)
    if "team_code" in dir_df.columns and "team_id" in dir_df.columns:
        m = batters.merge(dir_df[["team_code", "team_id"]].drop_duplicates(),
                          on="team_code", how="left")
        batters["team_id"] = batters.get("team_id", pd.Series([pd.NA]*len(batters)))
        batters.loc[batters["team_id"].isna(), "team_id"] = m.loc[batters["team_id"].isna(), "team_id"]
    return batters

def main(batters_path: str, games_path: str, output_path: str) -> None:
    batters = pd.read_csv(batters_path, dtype=str, keep_default_na=False)
    games = pd.read_csv(games_path, dtype=str, keep_default_na=False)

    batters = strip_strings(batters)
    games = strip_strings(games)

    # OPTIONAL: backfill team_id if missing
    batters = fill_team_id_from_directory(batters)

    if "team_id" not in batters.columns:
        batters.assign(side="home").to_csv(output_path, index=False)
        return

    batters["team_id"] = pd.to_numeric(batters["team_id"], errors="coerce").astype("Int64")
    games["home_team_id"] = pd.to_numeric(games.get("home_team_id"), errors="coerce").astype("Int64")
    games["away_team_id"] = pd.to_numeric(games.get("away_team_id"), errors="coerce").astype("Int64")

    b_keyed = batters.dropna(subset=["team_id"]).copy()
    g_keyed = games.dropna(subset=["home_team_id"]).copy()

    merged = b_keyed.merge(
        g_keyed,
        left_on="team_id",
        right_on="home_team_id",
        how="inner",
        validate="m:1",
        suffixes=("", "_g"),
    )

    merged["side"] = "home"
    merged["opponent_team_id"] = merged["away_team_id"]

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
            "data/adjusted/batters_home.csv",
        )
    main(batters_file, games_file, output_file)
