#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/apply_pitcher_park_adjustment.py
#
# Purpose:
# - Merge pitcher splits (home/away) with park factors by game_id.
# - Inject pitcher woba from data/Data/pitchers.csv, fallback data/manual/pitchersWoba.csv.
# - Default any remaining missing woba to 0.320 and log to summaries/pitchers_adjust/missing_woba.txt.
# - Write adj_woba_park = woba * (park_factor / 100).
# - Commit outputs.

import os
import pandas as pd
import subprocess
from pathlib import Path

# Inputs
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
GAMES_FILE         = "data/raw/todaysgames_normalized.csv"

# wOBA sources (primary then fallback)
PITCHERS_MASTER    = "data/Data/pitchers.csv"
PITCHERS_FALLBACK  = "data/manual/pitchersWoba.csv"

# Outputs
OUTPUT_HOME = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_park.csv"

# Logs
LOG_HOME    = "log_pitchers_home_park.txt"
LOG_AWAY    = "log_pitchers_away_park.txt"
LOG_MISSING = "summaries/pitchers_adjust/missing_woba.txt"

REQUIRED_PITCHER_KEYS = {"player_id", "game_id"}
REQUIRED_GAMES_COLS   = {"game_id", "park_factor"}
W_DEFAULT             = 0.320  # neutral league-average wOBA allowed


def validate_columns(df: pd.DataFrame, required: set, source_path: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{source_path} missing columns: {missing}")


def load_woba_map() -> pd.DataFrame:
    frames = []

    if os.path.exists(PITCHERS_MASTER):
        df1 = pd.read_csv(PITCHERS_MASTER, low_memory=False)
        if "player_id" in df1.columns and "woba" in df1.columns:
            frames.append(df1[["player_id", "woba"]].copy())

    if os.path.exists(PITCHERS_FALLBACK):
        df2 = pd.read_csv(PITCHERS_FALLBACK, low_memory=False)
        cols_lower = {c.lower(): c for c in df2.columns}
        woba_col = cols_lower.get("woba")
        if woba_col and "player_id" in df2.columns:
            frames.append(df2[["player_id", woba_col]].rename(columns={woba_col: "woba"}))

    if not frames:
        return pd.DataFrame(columns=["player_id", "woba"])

    woba = pd.concat(frames, ignore_index=True)
    woba["player_id"] = pd.to_numeric(woba["player_id"], errors="coerce").astype("Int64")
    woba["woba"] = pd.to_numeric(woba["woba"], errors="coerce")
    woba = woba.dropna(subset=["player_id"]).drop_duplicates(subset=["player_id"], keep="last")
    return woba


def merge_and_adjust(pitch_df: pd.DataFrame, games_df: pd.DataFrame,
                     woba_map: pd.DataFrame, side_label: str) -> pd.DataFrame:
    # Merge park factor by game_id
    merged = pitch_df.merge(
        games_df[["game_id", "park_factor"]],
        on="game_id",
        how="left",
        validate="m:1"
    )

    # Inject woba via player_id
    merged = merged.merge(
        woba_map,
        on="player_id",
        how="left",
        validate="m:1"
    )

    # Default missing woba and log
    missing_mask = merged["woba"].isna()
    if missing_mask.any():
        Path(LOG_MISSING).parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_MISSING, "a", encoding="utf-8") as f:
            for _, r in merged.loc[missing_mask, ["player_id", "game_id"]].drop_duplicates().iterrows():
                f.write(
                    f"[park/{side_label}] used_default_woba={W_DEFAULT} "
                    f"player_id={r['player_id']} game_id={r['game_id']}\n"
                )
        merged.loc[missing_mask, "woba"] = W_DEFAULT

    # Compute adjusted value (park_factor given like 104 -> 1.04)
    merged["adj_woba_park"] = merged["woba"] * (pd.to_numeric(merged["park_factor"], errors="coerce") / 100.0)

    return merged


def log_top5(df: pd.DataFrame, log_path: str, label: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"Top 5 {label} pitchers by adj_woba_park\n")
        cols_pref = ["pitcher_name", "player_id", "game_id", "woba", "park_factor", "adj_woba_park"]
        cols = [c for c in cols_pref if c in df.columns]
        top5 = df.sort_values("adj_woba_park", ascending=False).head(5)
        f.write("\n")
        if len(cols):
            f.write(top5[cols].to_string(index=False))
        else:
            f.write("no columns available")


def git_commit_and_push(paths) -> None:
    try:
        subprocess.run(["git", "add"] + paths, check=True)
        status = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if status.strip():
            subprocess.run(["git", "commit", "-m", "Apply pitcher park factor with woba injection + defaults"], check=True)
            subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}")


def main() -> None:
    # Load inputs
    for p in [PITCHERS_HOME_FILE, PITCHERS_AWAY_FILE, GAMES_FILE]:
        if not os.path.exists(p):
            print(f"CANNOT COMPLY: Missing required input file: {p}")
            return

    try:
        home_df  = pd.read_csv(PITCHERS_HOME_FILE)
        away_df  = pd.read_csv(PITCHERS_AWAY_FILE)
        games_df = pd.read_csv(GAMES_FILE)
    except Exception as e:
        print(f"CANNOT COMPLY: Failed to read input CSVs: {e}")
        return

    # Basic validations
    try:
        validate_columns(home_df, REQUIRED_PITCHER_KEYS, PITCHERS_HOME_FILE)
        validate_columns(away_df, REQUIRED_PITCHER_KEYS, PITCHERS_AWAY_FILE)
        validate_columns(games_df, REQUIRED_GAMES_COLS, GAMES_FILE)
    except ValueError as e:
        print(f"CANNOT COMPLY: {e}")
        return

    # Normalize keys
    for df in (home_df, away_df, games_df):
        if "player_id" in df.columns:
            df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        if "game_id" in df.columns:
            df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    games_df["park_factor"] = pd.to_numeric(games_df["park_factor"], errors="coerce")

    # wOBA map
    woba_map = load_woba_map()

    # Process
    adjusted_home = merge_and_adjust(home_df, games_df, woba_map, "home")
    adjusted_away = merge_and_adjust(away_df, games_df, woba_map, "away")

    # Save
    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    Path(OUTPUT_AWAY).parent.mkdir(parents=True, exist_ok=True)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    # Logs
    log_top5(adjusted_home, LOG_HOME, "home")
    log_top5(adjusted_away, LOG_AWAY, "away")

    # Commit
    git_commit_and_push([OUTPUT_HOME, OUTPUT_AWAY, LOG_HOME, LOG_AWAY, LOG_MISSING])


if __name__ == "__main__":
    main()
