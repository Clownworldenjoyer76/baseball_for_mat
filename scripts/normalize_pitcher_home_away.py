#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/normalize_pitcher_home_away.py

Normalize pitcher records into home/away CSVs using team_directory.csv.
Guarantee ID columns export as plain integers (no decimals).
"""

import pandas as pd
import logging
from pathlib import Path
import sys
import os

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TEAM_MAP_FILE = Path("data/manual/team_directory.csv")

ID_COLS_OUT = ["player_id", "game_id"]  # will render to digit-only strings if present

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

def load_team_map() -> dict:
    if not TEAM_MAP_FILE.exists():
        raise FileNotFoundError(f"{TEAM_MAP_FILE} does not exist.")
    df = pd.read_csv(TEAM_MAP_FILE)
    required = {"team_code", "team_name"}
    if not required.issubset(df.columns):
        raise ValueError("team_directory.csv must contain: team_code, team_name")
    df["team_code"] = df["team_code"].astype(str).str.strip()
    df["team_name"] = df["team_name"].astype(str).str.strip()
    return dict(zip(df["team_code"], df["team_name"]))

def process_pitcher_data(pitchers_input_path: Path,
                         games_input_path: Path,
                         output_home_path: Path,
                         output_away_path: Path):
    team_map = load_team_map()

    if not pitchers_input_path.exists():
        raise FileNotFoundError(f"{pitchers_input_path} does not exist.")
    pitchers_df = pd.read_csv(pitchers_input_path)
    pitchers_df = strip_strings(pitchers_df)
    # Normalize name field and drop dup rows to stabilize matches
    if "name" in pitchers_df.columns:
        pitchers_df["name"] = pitchers_df["name"].astype(str).str.strip()
    pitchers_df = pitchers_df.drop_duplicates()

    if not games_input_path.exists():
        raise FileNotFoundError(f"{games_input_path} does not exist.")
    games_cols = ["pitcher_home", "pitcher_away", "home_team", "away_team", "game_id"]
    full_games_df = pd.read_csv(games_input_path)
    missing = [c for c in games_cols if c not in full_games_df.columns]
    if missing:
        raise RuntimeError(f"{games_input_path}: missing columns {missing}")
    full_games_df = full_games_df[games_cols]
    full_games_df = strip_strings(full_games_df)

    # Map codes -> names for home/away team labels
    full_games_df["home_team"] = (
        full_games_df["home_team"].astype(str).str.strip()
        .map(team_map).fillna(full_games_df["home_team"])
    )
    full_games_df["away_team"] = (
        full_games_df["away_team"].astype(str).str.strip()
        .map(team_map).fillna(full_games_df["away_team"])
    )

    # Build HOME pitchers
    home_tagged = []
    for _, row in full_games_df.iterrows():
        p = row["pitcher_home"]
        h = row["home_team"]
        a = row["away_team"]
        g = row["game_id"]
        matched = pitchers_df[pitchers_df.get("name", pd.Series(dtype=str)) == p].copy()
        if not matched.empty:
            matched["team"] = h
            matched["home_away"] = "home"
            matched["game_home_team"] = h
            matched["game_away_team"] = a
            matched["game_id"] = g
            home_tagged.append(matched)

    home_df = pd.concat(home_tagged, ignore_index=True) if home_tagged else pd.DataFrame()
    if not home_df.empty:
        home_df.drop(columns=[c for c in home_df.columns if c.endswith(".1")],
                     errors="ignore", inplace=True)
        home_df = strip_strings(home_df)
        home_df.drop_duplicates(inplace=True)

    # Build AWAY pitchers
    away_tagged = []
    for _, row in full_games_df.iterrows():
        p = row["pitcher_away"]
        h = row["home_team"]
        a = row["away_team"]
        g = row["game_id"]
        matched = pitchers_df[pitchers_df.get("name", pd.Series(dtype=str)) == p].copy()
        if not matched.empty:
            matched["team"] = a
            matched["home_away"] = "away"
            matched["game_home_team"] = h
            matched["game_away_team"] = a
            matched["game_id"] = g
            away_tagged.append(matched)

    away_df = pd.concat(away_tagged, ignore_index=True) if away_tagged else pd.DataFrame()
    if not away_df.empty:
        away_df.drop(columns=[c for c in away_df.columns if c.endswith(".1")],
                     errors="ignore", inplace=True)
        away_df = strip_strings(away_df)
        away_df.drop_duplicates(inplace=True)

    # Coerce IDs to Int64 in memory (if present), then render as digit strings for CSV
    id_candidates = set(ID_COLS_OUT) & set(home_df.columns)
    if home_df is not None and len(home_df) > 0 and id_candidates:
        home_df = to_int64(home_df, list(id_candidates))
        home_df = ints_to_digit_strings(home_df, list(id_candidates))
    id_candidates = set(ID_COLS_OUT) & set(away_df.columns)
    if away_df is not None and len(away_df) > 0 and id_candidates:
        away_df = to_int64(away_df, list(id_candidates))
        away_df = ints_to_digit_strings(away_df, list(id_candidates))

    os.makedirs(output_home_path.parent, exist_ok=True)
    os.makedirs(output_away_path.parent, exist_ok=True)
    if not home_df.empty:
        home_df.to_csv(output_home_path, index=False)
    if not away_df.empty:
        away_df.to_csv(output_away_path, index=False)

    logger.info(f"Home rows: {len(home_df)} | Away rows: {len(away_df)}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python normalize_pitcher_home_away.py <pitchers_input> <games_input> <out_home> <out_away>")
        sys.exit(1)
    process_pitcher_data(
        Path(sys.argv[1]),
        Path(sys.argv[2]),
        Path(sys.argv[3]),
        Path(sys.argv[4]),
    )
