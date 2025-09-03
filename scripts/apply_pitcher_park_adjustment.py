#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/apply_pitcher_park_adjustment.py

import os
import pandas as pd
import subprocess
from pathlib import Path

# Inputs
PITCHERS_HOME_FILE   = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE   = "data/adjusted/pitchers_away.csv"
GAMES_FILE           = "data/raw/todaysgames_normalized.csv"  # includes park_factor
PITCHERS_WOBA_FILE   = "data/Data/pitchers.csv"               # source of pitcher woba

# Outputs
OUTPUT_HOME = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_park.csv"
LOG_HOME    = "log_pitchers_home_park.txt"
LOG_AWAY    = "log_pitchers_away_park.txt"

REQUIRED_PITCHER_KEYS = {"player_id", "game_id"}
REQUIRED_GAMES_COLS   = {"game_id", "park_factor"}
REQUIRED_WOBA_COLS    = {"player_id", "woba"}                 # optional 'year' for dedupe

def _validate_cols(df: pd.DataFrame, required: set, src: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{src} missing columns: {missing}")

def _strip(df: pd.DataFrame) -> pd.DataFrame:
    obj = df.select_dtypes(include=["object"]).columns
    if len(obj):
        df[obj] = df[obj].apply(lambda s: s.str.strip())
        df[obj] = df[obj].replace({"": pd.NA})
    return df

def _to_int64(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def _ints_to_digit_str(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64").astype("string").replace({"<NA>": ""})
    return df

def _prepare_woba_map(woba_df: pd.DataFrame) -> pd.DataFrame:
    base_cols = ["player_id", "woba"]
    if "year" in woba_df.columns:
        woba_df = woba_df.copy()
        woba_df = _to_int64(woba_df, ["player_id", "year"])
        woba_df = woba_df.dropna(subset=["player_id", "woba"])
        woba_df = woba_df.sort_values(["player_id", "year"], ascending=[True, False])
        woba_df = woba_df.drop_duplicates(subset=["player_id"], keep="first")
        return woba_df[base_cols]
    else:
        woba_df = woba_df.copy()
        woba_df = _to_int64(woba_df, ["player_id"])
        woba_df = woba_df.dropna(subset=["player_id", "woba"])
        woba_df = woba_df.drop_duplicates(subset=["player_id"], keep="first")
        return woba_df[base_cols]

def _apply_park(pitch_df: pd.DataFrame, games_df: pd.DataFrame, woba_map: pd.DataFrame) -> pd.DataFrame:
    pitch_df  = _to_int64(pitch_df, ["player_id", "game_id"])
    games_df  = _to_int64(games_df, ["game_id"])

    # Merge park factor by game_id
    merged = pitch_df.merge(games_df[["game_id", "park_factor"]], on="game_id", how="left", validate="m:1")

    # Merge woba by player_id
    merged = merged.merge(woba_map, on="player_id", how="left", validate="m:1")

    # Compute adjusted
    if "woba" in merged.columns and "park_factor" in merged.columns:
        merged["adj_woba_park"] = merged["woba"] * merged["park_factor"]
    else:
        merged["adj_woba_park"] = pd.NA

    # ID columns as digit strings
    merged = _ints_to_digit_str(merged, ["player_id", "game_id"])
    return merged

def _log_top5(df: pd.DataFrame, log_path: str, label: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as f:
        f.write(f"Top 5 {label} pitchers by adj_woba_park\n")
        cols_pref = ["pitcher_name", "player_id", "game_id", "woba", "park_factor", "adj_woba_park", "team", "home_away"]
        cols = [c for c in cols_pref if c in df.columns]
        try:
            top5 = df.sort_values("adj_woba_park", ascending=False).head(5)
            f.write(top5[cols].to_string(index=False))
        except Exception:
            f.write("No sortable data.\n")

def _git_commit(files, message: str) -> None:
    try:
        subprocess.run(["git", "add", *files], check=True)
        status = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if status.strip():
            subprocess.run(["git", "commit", "-m", message], check=True)
            subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}")

def main() -> None:
    needed = [PITCHERS_HOME_FILE, PITCHERS_AWAY_FILE, GAMES_FILE, PITCHERS_WOBA_FILE]
    if any(not os.path.exists(p) for p in needed):
        print("CANNOT COMPLY: Missing required input file(s). Expected:")
        for p in needed:
            print(f" - {p}")
        return

    try:
        home_df  = _strip(pd.read_csv(PITCHERS_HOME_FILE))
        away_df  = _strip(pd.read_csv(PITCHERS_AWAY_FILE))
        games_df = _strip(pd.read_csv(GAMES_FILE))
        woba_df  = _strip(pd.read_csv(PITCHERS_WOBA_FILE))
    except Exception as e:
        print(f"CANNOT COMPLY: Failed to read input CSVs: {e}")
        return

    try:
        _validate_cols(home_df, REQUIRED_PITCHER_KEYS, PITCHERS_HOME_FILE)
        _validate_cols(away_df, REQUIRED_PITCHER_KEYS, PITCHERS_AWAY_FILE)
        _validate_cols(games_df, REQUIRED_GAMES_COLS, GAMES_FILE)
        _validate_cols(woba_df, REQUIRED_WOBA_COLS, PITCHERS_WOBA_FILE)
    except ValueError as e:
        print(f"CANNOT COMPLY: {e}")
        return

    woba_map = _prepare_woba_map(woba_df)

    adj_home = _apply_park(home_df, games_df, woba_map)
    adj_away = _apply_park(away_df, games_df, woba_map)

    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adj_home.to_csv(OUTPUT_HOME, index=False)
    Path(OUTPUT_AWAY).parent.mkdir(parents=True, exist_ok=True)
    adj_away.to_csv(OUTPUT_AWAY, index=False)

    _log_top5(adj_home, LOG_HOME, "home")
    _log_top5(adj_away, LOG_AWAY, "away")

    _git_commit(
        [OUTPUT_HOME, OUTPUT_AWAY, LOG_HOME, LOG_AWAY],
        "Apply pitcher park adjustments (woba injection from data/Data/pitchers.csv)"
    )

if __name__ == "__main__":
    main()
