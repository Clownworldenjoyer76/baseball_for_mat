#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/inject_pitcher_ids_into_games.py

Populate pitcher_home_id and pitcher_away_id in data/raw/todaysgames_normalized.csv
by exact-name merge with data/normalized/pitchers_normalized.csv.

Strict rules:
- No fuzzy matching. Exact string match on the 'name' column.
- Output IDs as digit-only strings (no decimals), blank if unknown.
"""

import pandas as pd
from pathlib import Path

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
PITCHERS_NORM_FILE = Path("data/normalized/pitchers_normalized.csv")
OUT_FILE = Path("data/raw/todaysgames_normalized.csv")  # in-place update

REQ_G_COLS = {"pitcher_home", "pitcher_away"}
REQ_P_COLS = {"name", "player_id"}

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    obj = df.select_dtypes(include=["object"]).columns
    if len(obj):
        df[obj] = df[obj].apply(lambda s: s.str.strip())
        df[obj] = df[obj].replace({"": pd.NA})
    return df

def to_digit_str(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype("Int64").astype("string")
    return s.replace({"<NA>": ""})

def main():
    # Load & validate inputs
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"{GAMES_FILE} not found")
    if not PITCHERS_NORM_FILE.exists():
        raise FileNotFoundError(f"{PITCHERS_NORM_FILE} not found")

    games = pd.read_csv(GAMES_FILE)
    pitchers = pd.read_csv(PITCHERS_NORM_FILE)

    missing_g = REQ_G_COLS - set(games.columns)
    if missing_g:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(missing_g)}")

    missing_p = REQ_P_COLS - set(pitchers.columns)
    if missing_p:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {PITCHERS_NORM_FILE} missing columns: {sorted(missing_p)}")

    # Normalize strings
    games = strip_strings(games)
    pitchers = strip_strings(pitchers)

    # Build {name -> player_id} map (exact match)
    name_to_id = pitchers.set_index("name")["player_id"]

    # Map IDs
    games["pitcher_home_id"] = games["pitcher_home"].map(name_to_id)
    games["pitcher_away_id"] = games["pitcher_away"].map(name_to_id)

    # Render as digit-only strings
    games["pitcher_home_id"] = to_digit_str(games["pitcher_home_id"])
    games["pitcher_away_id"] = to_digit_str(games["pitcher_away_id"])

    # Save in place
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(OUT_FILE, index=False)
    print(f"✅ Injected pitcher IDs -> {OUT_FILE}")
    # Optional visibility
    resolved = games[["game_id"] + [c for c in ["pitcher_home","pitcher_home_id","pitcher_away","pitcher_away_id"] if c in games.columns]]
    print(resolved.to_string(index=False))

if __name__ == "__main__":
    main()
