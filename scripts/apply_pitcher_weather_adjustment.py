#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/apply_pitcher_weather_adjustment.py

import os
import pandas as pd
import subprocess
from pathlib import Path

# Inputs
PITCHERS_HOME_FILE   = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE   = "data/adjusted/pitchers_away.csv"
WEATHER_FILE         = "data/weather_adjustments.csv"
PITCHERS_WOBA_FILE   = "data/Data/pitchers.csv"   # source of pitcher woba

# Outputs
OUTPUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_weather.csv"
LOG_HOME    = "log_pitchers_home_weather.txt"
LOG_AWAY    = "log_pitchers_away_weather.txt"

REQUIRED_PITCHER_KEYS   = {"player_id", "game_id"}          # we inject woba
REQUIRED_WEATHER_COLS   = {"game_id", "weather_factor"}
REQUIRED_WOBA_COLS      = {"player_id", "woba"}             # optional 'year' for dedupe

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
    # Keep latest year per player_id if 'year' exists; else just drop duplicates
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

def _apply_weather(pitch_df: pd.DataFrame, weather_df: pd.DataFrame, woba_map: pd.DataFrame) -> pd.DataFrame:
    # Ensure keys
    pitch_df = _to_int64(pitch_df, ["player_id", "game_id"])
    weather_df = _to_int64(weather_df, ["game_id"])

    # Join woba by player_id
    merged = pitch_df.merge(woba_map, on="player_id", how="left", validate="m:1")

    # Join weather_factor by game_id
    merged = merged.merge(
        weather_df[["game_id", "weather_factor"]],
        on="game_id",
        how="left",
        validate="m:1"
    )

    # Compute adjusted
    if "woba" in merged.columns and "weather_factor" in merged.columns:
        merged["adj_woba_weather"] = merged["woba"] * merged["weather_factor"]
    else:
        merged["adj_woba_weather"] = pd.NA

    # ID columns as digit strings
    merged = _ints_to_digit_str(merged, ["player_id", "game_id"])
    return merged

def _log_top5(df: pd.DataFrame, log_path: str, label: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as f:
        f.write(f"Top 5 {label} pitchers by adj_woba_weather\n")
        cols_pref = ["pitcher_name", "player_id", "game_id", "woba", "weather_factor", "adj_woba_weather", "team", "home_away"]
        cols = [c for c in cols_pref if c in df.columns]
        try:
            top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
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
    # Basic presence
    needed = [PITCHERS_HOME_FILE, PITCHERS_AWAY_FILE, WEATHER_FILE, PITCHERS_WOBA_FILE]
    if any(not os.path.exists(p) for p in needed):
        print("CANNOT COMPLY: Missing required input file(s). Expected:")
        for p in needed:
            print(f" - {p}")
        return

    # Load
    try:
        home_df   = _strip(pd.read_csv(PITCHERS_HOME_FILE))
        away_df   = _strip(pd.read_csv(PITCHERS_AWAY_FILE))
        weather_df= _strip(pd.read_csv(WEATHER_FILE))
        woba_df   = _strip(pd.read_csv(PITCHERS_WOBA_FILE))
    except Exception as e:
        print(f"CANNOT COMPLY: Failed to read input CSVs: {e}")
        return

    # Validate columns
    try:
        _validate_cols(home_df, REQUIRED_PITCHER_KEYS, PITCHERS_HOME_FILE)
        _validate_cols(away_df, REQUIRED_PITCHER_KEYS, PITCHERS_AWAY_FILE)
        _validate_cols(weather_df, REQUIRED_WEATHER_COLS, WEATHER_FILE)
        _validate_cols(woba_df, REQUIRED_WOBA_COLS, PITCHERS_WOBA_FILE)
    except ValueError as e:
        print(f"CANNOT COMPLY: {e}")
        return

    # Prepare woba map (latest year per player if available)
    woba_map = _prepare_woba_map(woba_df)

    # Apply weather factor
    adjusted_home = _apply_weather(home_df, weather_df, woba_map)
    adjusted_away = _apply_weather(away_df, weather_df, woba_map)

    # Save
    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    Path(OUTPUT_AWAY).parent.mkdir(parents=True, exist_ok=True)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    # Logs
    _log_top5(adjusted_home, LOG_HOME, "home")
    _log_top5(adjusted_away, LOG_AWAY, "away")

    # Commit
    _git_commit(
        [OUTPUT_HOME, OUTPUT_AWAY, LOG_HOME, LOG_AWAY],
        "Apply pitcher weather factor by game_id with woba injection from data/Data/pitchers.csv"
    )

if __name__ == "__main__":
    main()
