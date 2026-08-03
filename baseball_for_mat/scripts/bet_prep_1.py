#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/bet_prep_1.py

Build mlb_sched.csv using normalized games and stadium master.

Inputs:
  - data/raw/todaysgames_normalized.csv
      required: game_id, home_team_id, away_team_id, home_team, away_team
      optional: game_datetime (UTC ISO)

  - data/manual/stadium_master.csv
      required: team_id, venue

Output:
  - data/bets/mlb_sched.csv
      columns: game_id, date, home_team, home_team_id, away_team, away_team_id, venue_name
"""

from pathlib import Path
import pandas as pd

GAMES_FILE   = Path("data/raw/todaysgames_normalized.csv")
STADIUM_FILE = Path("data/manual/stadium_master.csv")
OUTPUT_FILE  = Path("data/bets/mlb_sched.csv")

REQ_GAMES   = ["game_id", "home_team_id", "away_team_id", "home_team", "away_team"]
REQ_STADIUM = ["team_id", "venue"]

def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def required(df: pd.DataFrame, cols: list[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{where}: missing required columns: {missing}")

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    obj = df.select_dtypes(include=["object"]).columns
    if len(obj):
        df[obj] = df[obj].apply(lambda s: s.str.strip())
        df[obj] = df[obj].replace({"": pd.NA})
    return df

def to_int64(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def ints_to_str_digits(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64").astype("string").replace({"<NA>": ""})
    return df

def compute_date_column(games: pd.DataFrame) -> pd.Series:
    # If game_datetime exists, convert UTC -> America/New_York; else use ET today.
    if "game_datetime" in games.columns:
        ts = pd.to_datetime(games["game_datetime"], utc=True, errors="coerce")
        return ts.dt.tz_convert("America/New_York").dt.date.astype("string")
    today_et = pd.Timestamp.now(tz="America/New_York").date().isoformat()
    return pd.Series([today_et] * len(games), index=games.index, dtype="string")

def main():
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"Missing input: {GAMES_FILE}")
    if not STADIUM_FILE.exists():
        raise FileNotFoundError(f"Missing input: {STADIUM_FILE}")

    games = pd.read_csv(GAMES_FILE, dtype=str, keep_default_na=False)
    stadium = pd.read_csv(STADIUM_FILE, dtype=str, keep_default_na=False)

    games = strip_strings(games)
    stadium = strip_strings(stadium)

    required(games, REQ_GAMES, str(GAMES_FILE))
    required(stadium, REQ_STADIUM, str(STADIUM_FILE))

    # Coerce IDs to Int64 internally
    games   = to_int64(games,   ["game_id", "home_team_id", "away_team_id"])
    stadium = to_int64(stadium, ["team_id"])

    # Date column
    games["date"] = compute_date_column(games)

    # Join venue by home_team_id (schedule) -> team_id (stadium master)
    merged = games.merge(
        stadium[["team_id", "venue"]],
        left_on="home_team_id",
        right_on="team_id",
        how="left",
        validate="m:1",
    ).drop(columns=["team_id"])

    merged.rename(columns={"venue": "venue_name"}, inplace=True)

    # Final schema
    out_cols = [
        "game_id",
        "date",
        "home_team",
        "home_team_id",
        "away_team",
        "away_team_id",
        "venue_name",
    ]
    for c in out_cols:
        if c not in merged.columns:
            merged[c] = pd.NA

    out = merged[out_cols].drop_duplicates()

    # Render IDs as digit-only strings for CSV
    out = ints_to_str_digits(out, ["game_id", "home_team_id", "away_team_id"])

    ensure_parent(OUTPUT_FILE)
    out.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote {len(out)} rows -> {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
