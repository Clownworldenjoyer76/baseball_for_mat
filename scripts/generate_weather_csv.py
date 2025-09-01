#!/usr/bin/env python3
"""
Build data/weather_input.csv from normalized games + stadium master.

Inputs
- data/raw/todaysgames_normalized.csv
    required: home_team_id, away_team_id, home_team, away_team, date, game_time, park_factor
- data/manual/stadium_master.csv
    required: team_id, venue, city, latitude, longitude, roof_type, time_of_day

Output
- data/weather_input.csv
    columns: home_team_id, away_team_id, home_team, away_team, date, game_time,
             park_factor, venue, city, latitude, longitude, roof_type, time_of_day
"""
from pathlib import Path
import pandas as pd

GAMES = Path("data/raw/todaysgames_normalized.csv")
STAD  = Path("data/manual/stadium_master.csv")   # ← use the master you saved
OUT   = Path("data/weather_input.csv")

REQ_G = ["home_team_id","away_team_id","home_team","away_team","date","game_time","park_factor"]
REQ_S = ["team_id","venue","city","latitude","longitude","roof_type","time_of_day"]

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

def required(df: pd.DataFrame, cols: list[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{where}: missing required columns: {missing}")

def main():
    if not GAMES.exists():
        raise FileNotFoundError(f"Missing input: {GAMES}")
    if not STAD.exists():
        raise FileNotFoundError(f"Missing input: {STAD}")

    g = pd.read_csv(GAMES, dtype=str, keep_default_na=False)
    s = pd.read_csv(STAD,  dtype=str, keep_default_na=False)

    g = strip_strings(g)
    s = strip_strings(s)

    required(g, REQ_G, str(GAMES))
    required(s, REQ_S, str(STAD))

    # Coerce IDs to Int64 for a clean merge, then back to strings at the end
    g  = to_int64(g, ["home_team_id","away_team_id"])
    s  = to_int64(s, ["team_id"])

    # Select minimal game fields needed downstream
    g_keep = ["home_team_id","away_team_id","home_team","away_team","date","game_time","park_factor"]
    g = g[g_keep].copy()

    # Stadium fields keyed by team_id (home team)
    s_keep = ["team_id","venue","city","latitude","longitude","roof_type","time_of_day"]
    s = s[s_keep].rename(columns={"team_id":"home_team_id"})

    # Merge by home_team_id (ID, not names)
    x = g.merge(s, on="home_team_id", how="left", validate="m:1")

    # Surface any missing required stadium fields
    req_after = ["venue","city","latitude","longitude","roof_type","time_of_day"]
    miss_mask = x[req_after].isna().any(axis=1)
    if miss_mask.any():
        print("⚠️ Warning: missing stadium fields for some rows (by home_team_id):")
        print(x.loc[miss_mask, ["home_team_id","home_team","away_team"] + req_after]
                .drop_duplicates()
                .to_string(index=False))

    # Render IDs back to digit-only strings
    for c in ["home_team_id","away_team_id"]:
        if c in x.columns:
            x[c] = x[c].astype("Int64").astype("string").replace({"<NA>": ""})

    # Column order for output
    out_cols = [
        "home_team_id","away_team_id","home_team","away_team",
        "date","game_time","park_factor",
        "venue","city","latitude","longitude","roof_type","time_of_day",
    ]
    # Ensure all exist
    for c in out_cols:
        if c not in x.columns:
            x[c] = pd.NA

    OUT.parent.mkdir(parents=True, exist_ok=True)
    x[out_cols].to_csv(OUT, index=False)
    print(f"✅ Wrote {len(x)} rows -> {OUT}")

if __name__ == "__main__":
    main()
