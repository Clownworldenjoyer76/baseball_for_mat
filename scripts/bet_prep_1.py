#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import pandas as pd

INPUT_FILE  = Path("data/raw/mlb_schedule_today.csv")
MAP_FILE    = Path("data/manual/team_directory.csv")
OUTPUT_FILE = Path("data/bets/mlb_sched.csv")

def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def required(df: pd.DataFrame, cols: list[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{where}: missing required columns: {missing}")

def to_et_date(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, utc=True, errors="coerce")
    return ts.dt.tz_convert("America/New_York").dt.date.astype("string")

def today_et_str() -> str:
    return pd.Timestamp.now(tz="America/New_York").date().isoformat()

def build_name_map(df_map: pd.DataFrame) -> dict:
    # Use multiple sources to map to team_name (spaced form), which
    # aligns with todaysgames_normalized.csv naming.
    required(df_map,
             ["team_name", "clean_team_name", "canonical_team"],
             "team_directory.csv")
    m = {}
    for _, r in df_map.iterrows():
        tn = str(r["team_name"]).strip()
        ct = str(r["canonical_team"]).strip()
        cl = str(r["clean_team_name"]).strip()
        # keys as lowercase for case-insensitive matching
        m[tn.lower()] = tn
        m[ct.lower()] = tn
        m[cl.lower()] = tn
    return m

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
    name_map = build_name_map(df_map)

    df["date"] = to_et_date(df["game_datetime"])
    et_today = today_et_str()
    df = df[df["date"] == et_today].copy()

    def canon(name: str) -> str:
        key = str(name).strip().lower()
        return name_map.get(key, str(name).strip())

    df["home_team"] = df["home_team_name"].map(canon)
    df["away_team"] = df["away_team_name"].map(canon)

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
        if c not in df.columns:
            df[c] = pd.NA
    df = df[out_cols].drop_duplicates()

    ensure_parent(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote {len(df)} rows for ET {et_today} -> {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
