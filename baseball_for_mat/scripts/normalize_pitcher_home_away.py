#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/normalize_pitcher_home_away.py

Export pitchers_home.csv and pitchers_away.csv directly from games file.
No name/team-string matching beyond fields present in games file.
Strict ID handling: player_id and game_id are written as digit-only strings.

Accepted CLI forms (backward compatible):
  NEW (preferred):
    python scripts/normalize_pitcher_home_away.py <games_input> <out_home> <out_away>

  LEGACY (ignored first arg to maintain workflow compatibility):
    python scripts/normalize_pitcher_home_away.py <_ignored_pitchers_input> <games_input> <out_home> <out_away>
"""

import sys
import os
from pathlib import Path
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "game_id",
    "home_team",
    "away_team",
    "game_time",
    "pitcher_home",
    "pitcher_away",
    "home_team_id",
    "away_team_id",
    "pitcher_home_id",
    "pitcher_away_id",
    "park_factor",
}

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
    missing = REQUIRED_COLUMNS - set(g.columns)
    if missing:
        raise RuntimeError(
            "INSUFFICIENT INFORMATION: data/raw/todaysgames_normalized.csv is missing required columns: "
            + ", ".join(sorted(missing))
        )

def build_side(games_df: pd.DataFrame, side: str) -> pd.DataFrame:
    if side not in ("home", "away"):
        raise ValueError("side must be 'home' or 'away'")

    pid_col = "pitcher_home_id" if side == "home" else "pitcher_away_id"
    pname_col = "pitcher_home" if side == "home" else "pitcher_away"

    # Base selection from games
    cols = [
        "game_id",
        "home_team",
        "away_team",
        pid_col,
        pname_col,
    ]
    g = games_df[cols].copy()

    # Coerce IDs, drop rows missing game_id or pitcher_id
    g = to_int64(g, ["game_id", pid_col]).dropna(subset=["game_id", pid_col])

    # Normalize output schema
    out = pd.DataFrame({
        "player_id": g[pid_col],
        "game_id": g["game_id"],
        "pitcher_name": g[pname_col],
        "game_home_team": g["home_team"],
        "game_away_team": g["away_team"],
    })

    # Attach team flag/context
    if side == "home":
        out["team"] = out["game_home_team"]
        out["home_away"] = "home"
    else:
        out["team"] = out["game_away_team"]
        out["home_away"] = "away"

    # Final cleanup: IDs as digit-only strings, trim strings, drop dups
    out = strip_strings(out).drop_duplicates()
    out = to_int64(out, ID_COLS_OUT)
    out = ints_to_digit_strings(out, ID_COLS_OUT)

    # Column order
    out = out[[
        "player_id",
        "game_id",
        "pitcher_name",
        "team",
        "home_away",
        "game_home_team",
        "game_away_team",
    ]]

    return out

def process(games_input_path: Path, output_home_path: Path, output_away_path: Path):
    if not games_input_path.exists():
        raise FileNotFoundError(f"{games_input_path} does not exist.")

    games_df = pd.read_csv(games_input_path)
    games_df = strip_strings(games_df)
    ensure_games_columns(games_df)

    home_df = build_side(games_df, "home")
    away_df = build_side(games_df, "away")

    os.makedirs(output_home_path.parent, exist_ok=True)
    os.makedirs(output_away_path.parent, exist_ok=True)

    home_df.to_csv(output_home_path, index=False)
    away_df.to_csv(output_away_path, index=False)

    logger.info(f"Home rows: {len(home_df)} | Away rows: {len(away_df)}")

if __name__ == "__main__":
    # NEW: 3 args (games, out_home, out_away)
    # LEGACY: 4 args (ignored_pitchers, games, out_home, out_away)
    if len(sys.argv) == 4:
        _, games_in, out_home, out_away = sys.argv
    elif len(sys.argv) == 5:
        # Ignore first arg for backward compatibility
        _, _ignored, games_in, out_home, out_away = sys.argv
    else:
        print("INSUFFICIENT INFORMATION: usage requires either 3 args <games_input> <out_home> <out_away> "
              "or 4 args <ignored_pitchers_input> <games_input> <out_home> <out_away>")
        sys.exit(1)

    process(Path(games_in), Path(out_home), Path(out_away))
