#!/usr/bin/env python3
"""
Build data/weather_input.csv with team IDs + stadium fields.

Inputs
- data/raw/todaysgames_normalized.csv
- data/manual/team_directory.csv
- data/manual/stadium_master.csv

Output
- data/weather_input.csv (columns: date, game_time, home_team_id, away_team_id, venue, city, latitude, longitude, roof_type, is_dome)
"""
import pandas as pd
from pathlib import Path
from datetime import datetime

GAMES = Path("data/raw/todaysgames_normalized.csv")
TEAMS = Path("data/manual/team_directory.csv")
STAD  = Path("data/manual/stadium_master.csv")
OUT   = Path("data/weather_input.csv")

def _norm(s):
    return str(s).strip().lower()

def _require(df, cols, where):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{where}: missing required columns: {miss}")

def _best_col(df, options):
    for c in options:
        if c in df.columns:
            return c
    return None

def _ensure_date_time(df):
    # Try to produce 'date' and 'game_time' columns
    date_col = _best_col(df, ["date", "game_date"])
    time_col = _best_col(df, ["game_time", "start_time", "time"])

    if date_col is None:
        # Try to parse from a combined datetime column if exists
        combo = _best_col(df, ["start_datetime", "game_datetime", "datetime"])
        if combo and combo in df.columns:
            dt = pd.to_datetime(df[combo], errors="coerce")
            df["date"] = dt.dt.strftime("%Y-%m-%d")
            df["game_time"] = dt.dt.strftime("%I:%M %p")
        else:
            # Fallback to today (ET not enforced here)
            today = datetime.now().strftime("%Y-%m-%d")
            df["date"] = today
    else:
        df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")

    if time_col is not None and "game_time" not in df.columns:
        # Normalize to e.g. "07:05 PM" if parsable
        parsed = pd.to_datetime(df[time_col], errors="coerce")
        mask = parsed.notna()
        df.loc[mask, "game_time"] = parsed[mask].dt.strftime("%I:%M %p")
        df.loc[~mask, "game_time"] = df.loc[~mask, time_col].astype(str)
    elif "game_time" not in df.columns:
        df["game_time"] = ""

    return df

def _build_team_id_map(team_df):
    # Accept flexible team directory schemas
    id_col = _best_col(team_df, ["team_id", "id"])
    name_col = _best_col(team_df, ["canonical_team", "team_code", "team", "name", "abbr"])
    if id_col is None or name_col is None:
        raise RuntimeError("team_directory: need team_id and a team name/code column")
    m = {}
    for n, i in zip(team_df[name_col], team_df[id_col]):
        if pd.isna(n) or pd.isna(i): 
            continue
        m[_norm(n)] = str(i).strip()
    return m

def _coalesce_team_id(df, side, team_map):
    # Prefer existing *_team_id; otherwise derive from name/code columns.
    id_col = _best_col(df, [f"{side}_team_id", f"{side}_id"])
    if id_col is not None:
        df[f"{side}_team_id"] = df[id_col].astype(str)
        return df

    name_col = _best_col(df, [f"{side}_team_canonical", f"{side}_team", side, f"{side}_name", f"{side}_team_code"])
    if name_col is None:
        df[f"{side}_team_id"] = None
        return df

    df[f"{side}_team_id"] = df[name_col].map(lambda s: team_map.get(_norm(s), None))
    return df

def _derive_is_dome(roof_type_series):
    s = roof_type_series.astype(str).str.lower().str.strip()
    return s.isin(["dome", "closed", "fixed", "indoor", "retractable-closed", "roof closed", "indoor dome"])

def main():
    g = pd.read_csv(GAMES)
    t = pd.read_csv(TEAMS)
    s = pd.read_csv(STAD)

    # Ensure required stadium fields exist
    _require(s, ["team_id", "venue", "city", "latitude", "longitude", "roof_type"], str(STAD))

    # Build team id map
    team_map = _build_team_id_map(t)

    # Normalize date/time
    g = _ensure_date_time(g)

    # Derive team IDs for both sides
    g = _coalesce_team_id(g, "home", team_map)
    g = _coalesce_team_id(g, "away", team_map)

    # Keep essentials from games
    keep_games = ["date", "game_time", "home_team_id", "away_team_id"]
    g = g[keep_games]

    # Join stadium by home_team_id
    s_trim = s[["team_id", "venue", "city", "latitude", "longitude", "roof_type"]].rename(columns={"team_id": "home_team_id"})
    x = g.merge(s_trim, on="home_team_id", how="left")

    # Derive is_dome flag
    x["is_dome"] = _derive_is_dome(x["roof_type"].fillna(""))

    # Surface missing join data
    req = ["date", "game_time", "home_team_id", "away_team_id", "venue", "city", "latitude", "longitude", "roof_type"]
    missing = x[req].isna().any(axis=1)
    if missing.any():
        bad = x.loc[missing, ["home_team_id", "away_team_id", "venue", "city"]].head(10)
        print("⚠️ Warning: missing stadium join for some rows. Examples:")
        print(bad.to_string(index=False))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    x.to_csv(OUT, index=False)

if __name__ == "__main__":
    main()
