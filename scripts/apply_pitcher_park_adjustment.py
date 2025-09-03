#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import subprocess
from pathlib import Path

# Inputs
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
PITCHERS_MAIN_FILE = "data/Data/pitchers.csv"
PITCHERS_WOBA_FILE = "data/manual/pitchersWoba.csv"

# Outputs
OUTPUT_HOME = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_park.csv"
LOG_HOME    = "log_pitchers_home_park.txt"
LOG_AWAY    = "log_pitchers_away_park.txt"

REQUIRED_PITCHER_COLS = {"game_id", "player_id", "park_factor"}

def load_woba_sources():
    frames = []
    if os.path.exists(PITCHERS_MAIN_FILE):
        frames.append(pd.read_csv(PITCHERS_MAIN_FILE, usecols=["player_id", "woba"]))
    if os.path.exists(PITCHERS_WOBA_FILE):
        frames.append(pd.read_csv(PITCHERS_WOBA_FILE, usecols=["player_id", "woba"]))
    if not frames:
        raise FileNotFoundError("No wOBA sources found.")
    return pd.concat(frames).dropna().drop_duplicates("player_id")

def inject_woba(pitch_df: pd.DataFrame, woba_df: pd.DataFrame) -> pd.DataFrame:
    if "woba" not in pitch_df.columns or pitch_df["woba"].isna().all():
        merged = pitch_df.merge(woba_df, on="player_id", how="left")
    else:
        merged = pitch_df.merge(woba_df, on="player_id", how="left", suffixes=("", "_src"))
        merged["woba"] = merged["woba"].fillna(merged["woba_src"])
        merged.drop(columns=["woba_src"], inplace=True)
    return merged

def apply_park_factor(pitch_df: pd.DataFrame) -> pd.DataFrame:
    pitch_df["adj_woba_park"] = pitch_df["woba"] * pitch_df["park_factor"]
    return pitch_df

def log_top5(df: pd.DataFrame, log_path: str, label: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as f:
        f.write(f"Top 5 {label} pitchers by adj_woba_park\n")
        if "adj_woba_park" in df.columns:
            top5 = df.sort_values("adj_woba_park", ascending=False).head(5)
            cols_pref = ["pitcher_name", "player_id", "game_id", "woba", "park_factor", "adj_woba_park"]
            cols = [c for c in cols_pref if c in top5.columns]
            f.write(top5[cols].to_string(index=False))
        else:
            f.write("adj_woba_park not present")

def git_commit_and_push() -> None:
    try:
        subprocess.run(["git", "add", OUTPUT_HOME, OUTPUT_AWAY, LOG_HOME, LOG_AWAY], check=True)
        status = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if status.strip():
            subprocess.run(["git", "commit", "-m", "Apply pitcher park adjustments with woba injection"], check=True)
            subprocess.run(["git", "push"], check=True)
        else:
            print("No changes to commit.")
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}")

def main() -> None:
    if not os.path.exists(PITCHERS_HOME_FILE) or not os.path.exists(PITCHERS_AWAY_FILE):
        print("CANNOT COMPLY: Missing pitcher input files.")
        return

    try:
        home_df   = pd.read_csv(PITCHERS_HOME_FILE)
        away_df   = pd.read_csv(PITCHERS_AWAY_FILE)
        woba_df   = load_woba_sources()
    except Exception as e:
        print(f"CANNOT COMPLY: Failed to read input CSVs: {e}")
        return

    for df in (home_df, away_df):
        missing = REQUIRED_PITCHER_COLS - set(df.columns)
        if missing:
            print(f"CANNOT COMPLY: Pitcher file missing columns: {missing}")
            return

    # Inject wOBA
    home_df = inject_woba(home_df, woba_df)
    away_df = inject_woba(away_df, woba_df)

    # Apply park factor
    adjusted_home = apply_park_factor(home_df)
    adjusted_away = apply_park_factor(away_df)

    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    log_top5(adjusted_home, LOG_HOME, "home")
    log_top5(adjusted_away, LOG_AWAY, "away")

    git_commit_and_push()

if __name__ == "__main__":
    main()
