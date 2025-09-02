#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/normalize_pitcher_home_away.py

Strict player_id-based export of pitchers_home.csv and pitchers_away.csv.
No name/team-string matching. Joins only on player_id (and attaches game_id).
ID columns are written as digit-only strings (no decimals).
Usage:
  python scripts/normalize_pitcher_home_away.py <pitchers_input> <games_input> <out_home> <out_away>
"""

import pandas as pd
import logging
from pathlib import Path
import sys
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ID_COLS_OUT = ["player_id", "game_id"]

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    obj = df.select_dtypes(include=["object"]).columns
    if len(obj):
        df[obj] = df[obj].apply(lambda s: s.str.strip())
        df[obj] = df[obj].replace({"": pd.NA})
    return df

def to_int64(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def ints_to_digit_strings(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64").astype("string").replace({"<NA>": ""})
    return df

def ensure_games_columns(g: pd.DataFrame):
    required = {"game_id", "home_team", "away_team", "pitcher_home_id", "pitcher_away_id"}
    missing = required - set(g.columns)
    if missing:
        raise RuntimeError(
            "INSUFFICIENT INFORMATION: data/raw/todaysgames_normalized.csv is missing required columns: "
            + ", ".join(sorted(missing))
        )

def build_side(pitchers_df: pd.DataFrame, games_df: pd.DataFrame, side: str) -> pd.DataFrame:
    # Map side -> column with pitcher ID
    pid_col = "pitcher_home_id" if side == "home" else "pitcher_away_id"

    # Select just needed game cols to avoid accidental merges on names
    g = games_df[["game_id", "home_team", "away_team", pid_col]].copy()
    g = to_int64(g, ["game_id", pid_col]).dropna(subset=["game_id", pid_col])

    # Prepare pitchers
    p = pitchers_df.copy()
    p = to_int64(p, ["player_id"]).dropna(subset=["player_id"])

    # Strict merge by player_id only; attach game metadata afterwards
    merged = p.merge(g, left_on="player_id", right_on=pid_col, how="inner", validate="m:m")

    # Attach team + home_away label using the games row
    if side == "home":
        merged["team"] = merged["home_team"]
        merged["home_away"] = "home"
        merged["game_home_team"] = merged["home_team"]
        merged["game_away_team"] = merged["away_team"]
    else:
        merged["team"] = merged["away_team"]
        merged["home_away"] = "away"
        merged["game_home_team"] = merged["home_team"]
        merged["game_away_team"] = merged["away_team"]

    # Keep a clean schema; drop helper key
    merged.drop(columns=[pid_col], inplace=True)
    merged = strip_strings(merged).drop_duplicates()

    # IDs as digit-only strings for CSV
    merged = to_int64(merged, ID_COLS_OUT)
    merged = ints_to_digit_strings(merged, ID_COLS_OUT)

    return merged

def process_pitcher_data(pitchers_input_path: Path,
                         games_input_path: Path,
                         output_home_path: Path,
                         output_away_path: Path):
    if not pitchers_input_path.exists():
        raise FileNotFoundError(f"{pitchers_input_path} does not exist.")
    if not games_input_path.exists():
        raise FileNotFoundError(f"{games_input_path} does not exist.")

    pitchers_df = pd.read_csv(pitchers_input_path)
    games_df = pd.read_csv(games_input_path)

    pitchers_df = strip_strings(pitchers_df)
    games_df = strip_strings(games_df)

    ensure_games_columns(games_df)

    home_df = build_side(pitchers_df, games_df, "home")
    away_df = build_side(pitchers_df, games_df, "away")

    os.makedirs(output_home_path.parent, exist_ok=True)
    os.makedirs(output_away_path.parent, exist_ok=True)

    home_df.to_csv(output_home_path, index=False)
    away_df.to_csv(output_away_path, index=False)

    logger.info(f"Home rows: {len(home_df)} | Away rows: {len(away_df)}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("INSUFFICIENT INFORMATION: usage requires 4 arguments — <pitchers_input> <games_input> <out_home> <out_away>")
        sys.exit(1)
    process_pitcher_data(
        Path(sys.argv[1]),
        Path(sys.argv[2]),
        Path(sys.argv[3]),
        Path(sys.argv[4]),
    )
