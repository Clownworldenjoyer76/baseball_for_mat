#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/normalize_pitcher_home_away.py

Normalize pitcher records into home/away CSVs using team_directory.csv.
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

def load_team_map() -> dict:
    if not TEAM_MAP_FILE.exists():
        logger.critical(f"❌ Missing team mapping file: {TEAM_MAP_FILE}")
        raise FileNotFoundError(f"{TEAM_MAP_FILE} does not exist.")
    df = pd.read_csv(TEAM_MAP_FILE)
    required = {"team_code", "team_name"}
    if not required.issuperset(set()) and not required.issubset(df.columns):
        raise ValueError("❌ team_directory.csv must have team_code, team_name")
    df["team_code"] = df["team_code"].astype(str).str.strip()
    df["team_name"] = df["team_name"].astype(str).str.strip()
    return dict(zip(df["team_code"], df["team_name"]))

def process_pitcher_data(pitchers_input_path: Path,
                         games_input_path: Path,
                         output_home_path: Path,
                         output_away_path: Path):
    logger.info("Starting pitcher data processing.")
    team_map = load_team_map()
    logger.info(f"Loaded team map entries: {len(team_map)}")

    if not pitchers_input_path.exists():
        raise FileNotFoundError(f"{pitchers_input_path} does not exist.")
    pitchers_df = pd.read_csv(pitchers_input_path)
    pitchers_df["name"] = pitchers_df["name"].astype(str).str.strip()
    pitchers_df = pitchers_df.drop_duplicates(subset=["name", "team"])

    if not games_input_path.exists():
        raise FileNotFoundError(f"{games_input_path} does not exist.")
    games_cols = ["pitcher_home", "pitcher_away", "home_team", "away_team"]
    full_games_df = pd.read_csv(games_input_path)[games_cols]
    full_games_df["home_team"] = (
        full_games_df["home_team"].astype(str).str.strip()
        .map(team_map).fillna(full_games_df["home_team"])
    )
    full_games_df["away_team"] = (
        full_games_df["away_team"].astype(str).str.strip()
        .map(team_map).fillna(full_games_df["away_team"])
    )

    home_tagged, home_missing, home_unmatched = [], [], []
    for _, row in full_games_df.iterrows():
        p = row["pitcher_home"]
        h = row["home_team"]
        a = row["away_team"]
        m = pitchers_df[pitchers_df["name"] == p].copy()
        if m.empty:
            home_missing.append(p)
            home_unmatched.append(h)
        else:
            m["team"] = h
            m["home_away"] = "home"
            m["game_home_team"] = h
            m["game_away_team"] = a
            home_tagged.append(m)

    home_df = (pd.concat(home_tagged, ignore_index=True)
               if home_tagged else pd.DataFrame())
    if not home_df.empty:
        home_df.drop(columns=[c for c in home_df.columns if c.endswith(".1")],
                     errors="ignore", inplace=True)
        home_df.sort_values(by=["team", "name"], inplace=True)
        home_df.drop_duplicates(inplace=True)
        home_df["team"] = (
            home_df["team"].astype(str).str.strip()
            .map(team_map).fillna(home_df["team"])
        )

    away_tagged, away_missing, away_unmatched = [], [], []
    for _, row in full_games_df.iterrows():
        p = row["pitcher_away"]
        h = row["home_team"]
        a = row["away_team"]
        m = pitchers_df[pitchers_df["name"] == p].copy()
        if m.empty:
            away_missing.append(p)
            away_unmatched.append(a)
        else:
            m["team"] = a
            m["home_away"] = "away"
            m["game_home_team"] = h
            m["game_away_team"] = a
            away_tagged.append(m)

    away_df = (pd.concat(away_tagged, ignore_index=True)
               if away_tagged else pd.DataFrame())
    if not away_df.empty:
        away_df.drop(columns=[c for c in away_df.columns if c.endswith(".1")],
                     errors="ignore", inplace=True)
        away_df.sort_values(by=["team", "name"], inplace=True)
        away_df.drop_duplicates(inplace=True)
        away_df["team"] = (
            away_df["team"].astype(str).str.strip()
            .map(team_map).fillna(away_df["team"])
        )

    os.makedirs(output_home_path.parent, exist_ok=True)
    os.makedirs(output_away_path.parent, exist_ok=True)
    if not home_df.empty:
        home_df.to_csv(output_home_path, index=False)
    if not away_df.empty:
        away_df.to_csv(output_away_path, index=False)

    raw_games_df = pd.read_csv(games_input_path)
    expected = len(raw_games_df) * 2
    actual = len(home_df) + len(away_df)
    if actual != expected:
        logger.warning(f"⚠️ Expected {expected}, got {actual} pitchers total.")
    else:
        logger.info(f"✅ Pitcher count OK: {actual}")

    if home_missing:
        logger.warning(f"Missing home pitchers: {len(set(home_missing))}")
    if away_missing:
        logger.warning(f"Missing away pitchers: {len(set(away_missing))}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        logger.critical(
            "Usage: python normalize_pitcher_home_away.py "
            "<pitchers_input> <games_input> <out_home> <out_away>"
        )
        sys.exit(1)
    process_pitcher_data(
        Path(sys.argv[1]),
        Path(sys.argv[2]),
        Path(sys.argv[3]),
        Path(sys.argv[4]),
    )
