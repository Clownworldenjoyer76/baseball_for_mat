#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

AWAY_FILE = Path("data/adjusted/pitchers_away.csv")
HOME_FILE = Path("data/adjusted/pitchers_home.csv")

ID_COLS = ["player_id", "game_id"]

def to_int64(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def ints_to_digit_strings(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64").astype("string").replace({"<NA>": ""})
    return df

def format_name(df: pd.DataFrame) -> pd.DataFrame:
    if "last_name" in df.columns and "first_name" in df.columns:
        df["name"] = (
            df["last_name"].astype(str).str.strip()
            + ", "
            + df["first_name"].astype(str).str.strip()
        )
    return df

def cleanup_away():
    df = pd.read_csv(AWAY_FILE)
    df = format_name(df)
    if "game_away_team" in df.columns:
        df.drop(columns=["game_away_team"], inplace=True, errors="ignore")
    if "game_home_team" in df.columns:
        df.rename(columns={"game_home_team": "home_team"}, inplace=True)

    df = to_int64(df, ID_COLS)
    df = ints_to_digit_strings(df, ID_COLS)

    df.to_csv(AWAY_FILE, index=False)
    print(f"✅ Cleaned: {AWAY_FILE}")

def cleanup_home():
    df = pd.read_csv(HOME_FILE)
    df = format_name(df)
    if "game_home_team" in df.columns:
        df.drop(columns=["game_home_team"], inplace=True, errors="ignore")
    if "game_away_team" in df.columns:
        df.rename(columns={"game_away_team": "away_team"}, inplace=True)

    df = to_int64(df, ID_COLS)
    df = ints_to_digit_strings(df, ID_COLS)

    df.to_csv(HOME_FILE, index=False)
    print(f"✅ Cleaned: {HOME_FILE}")

def main():
    cleanup_away()
    cleanup_home()

if __name__ == "__main__":
    main()
