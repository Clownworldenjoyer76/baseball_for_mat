#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

# Inputs
HOME_FILE  = Path("data/adjusted/batters_home.csv")
AWAY_FILE  = Path("data/adjusted/batters_away.csv")
GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")

ID_COLS = ["team_id", "home_team_id", "away_team_id", "game_id"]

def required(df: pd.DataFrame, cols: list[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{where}: missing columns {missing}")

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    obj = df.select_dtypes(include=["object"]).columns
    if len(obj):
        df[obj] = df[obj].apply(lambda s: s.str.strip())
        df[obj] = df[obj].replace({"": pd.NA})
    return df

def to_int64(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def int64_to_str_digits(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64").astype("string").replace({"<NA>": ""})
    return df

def first_series(df: pd.DataFrame, col: str) -> pd.Series | None:
    """Return the first occurrence of 'col' as a Series even if duplicates exist."""
    if col not in df.columns:
        return None
    obj = df.loc[:, [c for c in df.columns if c == col]]
    return obj if isinstance(obj, pd.Series) else obj.iloc[:, 0]

def drop_all(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Drop ALL columns matching 'name' (handles duplicates)."""
    return df.drop(columns=[c for c in df.columns if c == name], errors="ignore")

def dedupe_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Keep first occurrence when duplicate column names exist."""
    return df.loc[:, ~df.columns.duplicated()]

def merge_home(bh: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    m = bh.merge(
        games[["game_id", "home_team_id", "away_team_id", "home_team", "away_team"]],
        left_on="team_id",
        right_on="home_team_id",
        how="left",
        validate="m:1",
        suffixes=("", "_g"),
    )
    # Collapse duplicate columns first
    m = dedupe_cols(m)

    # Safely fill game_id from right side
    right_gid = first_series(m, "game_id_g")
    if right_gid is not None:
        left_gid = first_series(m, "game_id")
        if left_gid is not None:
            m["game_id"] = pd.to_numeric(left_gid, errors="coerce").astype("Int64") \
                .combine_first(pd.to_numeric(right_gid, errors="coerce").astype("Int64"))
        else:
            m["game_id"] = pd.to_numeric(right_gid, errors="coerce").astype("Int64")
        m = drop_all(m, "game_id_g")

    return m

def merge_away(ba: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    m = ba.merge(
        games[["game_id", "home_team_id", "away_team_id", "home_team", "away_team"]],
        left_on="team_id",
        right_on="away_team_id",
        how="left",
        validate="m:1",
        suffixes=("", "_g"),
    )
    # Collapse duplicate columns first
    m = dedupe_cols(m)

    # Safely fill game_id from right side
    right_gid = first_series(m, "game_id_g")
    if right_gid is not None:
        left_gid = first_series(m, "game_id")
        if left_gid is not None:
            m["game_id"] = pd.to_numeric(left_gid, errors="coerce").astype("Int64") \
                .combine_first(pd.to_numeric(right_gid, errors="coerce").astype("Int64"))
        else:
            m["game_id"] = pd.to_numeric(right_gid, errors="coerce").astype("Int64")
        m = drop_all(m, "game_id_g")

    return m

def main():
    # Read
    bh = pd.read_csv(HOME_FILE, dtype=str, keep_default_na=False)
    ba = pd.read_csv(AWAY_FILE, dtype=str, keep_default_na=False)
    g  = pd.read_csv(GAMES_FILE, dtype=str, keep_default_na=False)

    # Validate required columns
    required(bh, ["team_id"], str(HOME_FILE))
    required(ba, ["team_id"], str(AWAY_FILE))
    required(g,  ["game_id", "home_team_id", "away_team_id", "home_team", "away_team"], str(GAMES_FILE))

    # Clean and types
    bh = strip_strings(bh); ba = strip_strings(ba); g = strip_strings(g)
    bh = to_int64(bh, ["team_id", "game_id"])
    ba = to_int64(ba, ["team_id", "game_id"])
    g  = to_int64(g,  ["game_id", "home_team_id", "away_team_id"])

    # Merge by IDs
    bhm = merge_home(bh, g)
    bam = merge_away(ba, g)

    # Finalize ID formatting for CSV export
    bhm = int64_to_str_digits(bhm, ["team_id", "home_team_id", "away_team_id", "game_id"])
    bam = int64_to_str_digits(bam, ["team_id", "home_team_id", "away_team_id", "game_id"])

    # Safety: drop exact duplicate rows
    bhm.drop_duplicates(inplace=True)
    bam.drop_duplicates(inplace=True)

    # Write back
    bhm.to_csv(HOME_FILE, index=False)
    bam.to_csv(AWAY_FILE, index=False)
    print("✅ Updated batters_home.csv and batters_away.csv using robust ID-based merge and game_id fill.")

if __name__ == "__main__":
    main()
