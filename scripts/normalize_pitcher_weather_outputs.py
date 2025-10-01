#!/usr/bin/env python3
# scripts/normalize_pitcher_weather_outputs.py
#
# Purpose:
# - Ensure pitcher split files exist with required IDs
# - NOOP on values, but guarantee downstream merges won’t break
# - (New) assert uniqueness of game_id in data/weather_adjustments.csv before pitchers run
#
# Reads:
#   data/adjusted/pitchers_home.csv
#   data/adjusted/pitchers_away.csv
#   data/weather_adjustments.csv  (sanity check)
#
# Writes (pass-through for pitchers):
#   data/adjusted/pitchers_home.csv
#   data/adjusted/pitchers_away.csv

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

PITCHERS_HOME_IN  = Path("data/adjusted/pitchers_home.csv")
PITCHERS_AWAY_IN  = Path("data/adjusted/pitchers_away.csv")
PITCHERS_HOME_OUT = Path("data/adjusted/pitchers_home.csv")
PITCHERS_AWAY_OUT = Path("data/adjusted/pitchers_away.csv")

WEATHER_ADJ       = Path("data/weather_adjustments.csv")

REQ_PITCHER_COLS  = {"player_id", "game_id"}
REQ_WEATHER_COLS  = {"game_id", "weather_factor"}

def fail(msg: str, code: int = 1):
    print(msg)
    sys.exit(code)

def load_csv(p: Path, dtype=None) -> pd.DataFrame:
    if not p.exists():
        fail(f"Missing file: {p}")
    try:
        return pd.read_csv(p, dtype=dtype)
    except Exception as e:
        fail(f"Unable to read {p}: {e}", 2)

def ensure_cols(df: pd.DataFrame, needed: set[str], ctx: str):
    miss = [c for c in needed if c not in df.columns]
    if miss:
        fail(f"{ctx}: missing columns {miss}")

def main():
    # 1) Validate pitcher files have required IDs
    ph = load_csv(PITCHERS_HOME_IN)
    pa = load_csv(PITCHERS_AWAY_IN)
    ensure_cols(ph, REQ_PITCHER_COLS, str(PITCHERS_HOME_IN))
    ensure_cols(pa, REQ_PITCHER_COLS, str(PITCHERS_AWAY_IN))

    # 2) Sanity check the weather adjustments file that the pitcher step will use
    wx = load_csv(WEATHER_ADJ)
    ensure_cols(wx, REQ_WEATHER_COLS, str(WEATHER_ADJ))

    # Enforce uniqueness on game_id right here (belt-and-suspenders)
    if wx["game_id"].duplicated().any():
        # Collapse with mean to be consistent with finalize_weather_outputs
        wx_clean = (
            wx.groupby("game_id", as_index=False)["weather_factor"]
              .mean(numeric_only=True)
        )
        WEATHER_ADJ.parent.mkdir(parents=True, exist_ok=True)
        wx_clean.to_csv(WEATHER_ADJ, index=False)
        print(f"DEDUPED weather_adjustments.csv to one row per game_id (rows={len(wx_clean)})")
    else:
        print("weather_adjustments.csv already unique by game_id.")

    # 3) Pass-through writes (kept for pipeline consistency)
    PITCHERS_HOME_OUT.parent.mkdir(parents=True, exist_ok=True)
    ph.to_csv(PITCHERS_HOME_OUT, index=False)

    PITCHERS_AWAY_OUT.parent.mkdir(parents=True, exist_ok=True)
    pa.to_csv(PITCHERS_AWAY_OUT, index=False)

    print("normalize_pitcher_weather_outputs: OK")

if __name__ == "__main__":
    main()
