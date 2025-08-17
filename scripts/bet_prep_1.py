#!/usr/bin/env python3
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

# ---- Inputs / Output ----
INPUT_FILE   = Path("data/raw/mlb_schedule_today.csv")
MAP_FILE     = Path("data/Data/team_name_map.csv")   # columns: name, team
OUTPUT_FILE  = Path("data/bets/mlb_sched.csv")

# ---- Helpers ----
def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def required(df: pd.DataFrame, cols: list[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{where}: missing required columns: {missing}")

def to_et_date(series: pd.Series) -> pd.Series:
    # MLB gameDate is UTC ISO (e.g., 2025-08-16T23:05:00Z)
    ts = pd.to_datetime(series, utc=True, errors="coerce")
    # Convert to America/New_York via tz_convert; using IANA name keeps DST correct
    return ts.dt.tz_convert("America/New_York").dt.date.astype("string")

def today_et_str() -> str:
    # Compute 'today' in America/New_York
    # Use pandas for tz correctness
    now_et = pd.Timestamp.now(tz="America/New_York")
    return now_et.date().isoformat()

def build_map(df_map: pd.DataFrame) -> dict:
    # MAP_FILE has columns: name (source full name), team (desired short name)
    # Create a case-insensitive map on 'name'
    required(df_map, ["name", "team"], "team_name_map.csv")
    return {str(n).strip().lower(): t for n, t in zip(df_map["name"], df_map["team"])}

# ---- Main ----
def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)
    required(df,
        ["game_id", "game_datetime",
         "home_team_name", "home_team_id",
         "away_team_name", "away_team_id",
         "venue_name"],
        str(INPUT_FILE),
    )

    df_map = pd.read_csv(MAP_FILE)
    name_map = build_map(df_map)

    # Convert UTC -> ET, then take the ET calendar date
    df["date"] = to_et_date(df["game_datetime"])

    # Filter to ET today only
    et_today = today_et_str()
    df = df[df["date"] == et_today].copy()

    # Map canonical team names using map file (fallback to original if not found)
    def canon(name: str) -> str:
        key = str(name).strip().lower()
        return name_map.get(key, str(name).strip())

    df["home_team"] = df["home_team_name"].map(canon)
    df["away_team"] = df["away_team_name"].map(canon)

    # Final column order expected by downstream scripts
    out_cols = [
        "game_id",
        "date",
        "home_team",
        "home_team_id",
        "away_team",
        "away_team_id",
        "venue_name",
    ]

    # Align + write
    for c in out_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[out_cols].drop_duplicates()

    ensure_parent(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote {len(df)} rows for ET {et_today} -> {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
