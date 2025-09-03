#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# baseball_for_mat-main/scripts/apply_pitcher_weather_adjustment.py
#
# Merge per-game weather_factor into pitcher rows (home/away) by strict game_id.
# If 'woba' exists, compute adj_woba_weather = woba * weather_factor.
# If 'woba' is missing, still write weather_factor and an empty adj_woba_weather column.
# No assumptions beyond columns present.

import os
import pandas as pd
import subprocess
from pathlib import Path

# Inputs
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
WEATHER_FILE       = "data/weather_adjustments.csv"

# Outputs
OUTPUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_weather.csv"
LOG_HOME    = "log_pitchers_home_weather.txt"
LOG_AWAY    = "log_pitchers_away_weather.txt"

# Minimum required columns
REQUIRED_PITCHER_COLS = {"game_id"}
REQUIRED_WEATHER_COLS = {"game_id", "weather_factor"}

def validate_columns(df: pd.DataFrame, required: set, source_path: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{source_path} missing columns: {missing}")

def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def apply_weather_factor(pitch_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    # Merge strictly on game_id (many pitchers to one weather row)
    merged = pitch_df.merge(
        weather_df[["game_id", "weather_factor"]],
        on="game_id",
        how="left",
        validate="m:1"
    )

    # Ensure numeric for multiplication (preserves NaN if factor missing)
    merged = coerce_numeric(merged, ["weather_factor"])
    if "woba" in merged.columns:
        merged = coerce_numeric(merged, ["woba"])
        merged["adj_woba_weather"] = merged["woba"] * merged["weather_factor"]
    else:
        # Create empty column to preserve downstream schema
        merged["adj_woba_weather"] = pd.NA

    # Stable column order: keys/metrics first if present
    preferred = ["player_id", "game_id", "name", "woba", "weather_factor", "adj_woba_weather"]
    existing_pref = [c for c in preferred if c in merged.columns]
    remaining = [c for c in merged.columns if c not in existing_pref]
    merged = merged[existing_pref + remaining]
    return merged

def log_top5(df: pd.DataFrame, log_path: str, label: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as f:
        f.write(f"Top 5 {label} pitchers by adj_woba_weather\n")
        if "adj_woba_weather" in df.columns and df["adj_woba_weather"].notna().any():
            top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
            cols_pref = ["name", "player_id", "game_id", "woba", "weather_factor", "adj_woba_weather"]
            cols = [c for c in cols_pref if c in top5.columns]
            f.write("\n")
            f.write(top5[cols].to_string(index=False))
        else:
            f.write("\nNo non-empty adj_woba_weather values available.")

def git_commit_and_push() -> None:
    try:
        subprocess.run(["git", "add", OUTPUT_HOME, OUTPUT_AWAY, LOG_HOME, LOG_AWAY], check=True)
        status = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if status.strip():
            subprocess.run(["git", "commit", "-m", "Apply pitcher weather factor by game_id"], check=True)
            subprocess.run(["git", "push"], check=True)
        else:
            print("No changes to commit.")
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}")

def main() -> None:
    # Load guard
    missing_inputs = [p for p in (PITCHERS_HOME_FILE, PITCHERS_AWAY_FILE, WEATHER_FILE) if not os.path.exists(p)]
    if missing_inputs:
        print("CANNOT COMPLY: Missing required input file(s):")
        for p in missing_inputs:
            print(f" - {p}")
        return

    # Read
    try:
        home_df    = pd.read_csv(PITCHERS_HOME_FILE)
        away_df    = pd.read_csv(PITCHERS_AWAY_FILE)
        weather_df = pd.read_csv(WEATHER_FILE)
    except Exception as e:
        print(f"CANNOT COMPLY: Failed to read input CSVs: {e}")
        return

    # Validate required columns
    try:
        validate_columns(home_df, REQUIRED_PITCHER_COLS, PITCHERS_HOME_FILE)
        validate_columns(away_df, REQUIRED_PITCHER_COLS, PITCHERS_AWAY_FILE)
        validate_columns(weather_df, REQUIRED_WEATHER_COLS, WEATHER_FILE)
    except ValueError as e:
        print(f"CANNOT COMPLY: {e}")
        return

    # Process
    adjusted_home = apply_weather_factor(home_df, weather_df)
    adjusted_away = apply_weather_factor(away_df, weather_df)

    # Save
    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    Path(OUTPUT_AWAY).parent.mkdir(parents=True, exist_ok=True)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    # Logs
    log_top5(adjusted_home, LOG_HOME, "home")
    log_top5(adjusted_away, LOG_AWAY, "away")

    # Commit
    git_commit_and_push()

if __name__ == "__main__":
    main()
