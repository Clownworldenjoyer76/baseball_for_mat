#!/usr/bin/env python3
"""
fetch_mlb_ids.py  — updated to normalize dates in America/New_York and keep only ET 'today'

- Calls MLB StatsAPI schedule for a given date (default: ET today, unless MLB_DATE is set).
- Extracts gamePk -> game_id and team ids/abbrs/names.
- Normalizes schedule timestamps from UTC to America/New_York and creates a 'date' column (ET).
- Filters schedule rows to ET 'today' before saving snapshot (prevents mixed UTC spillover).
- Merges schedule metadata onto data/raw/todaysgames_normalized.csv using robust team-name keys.
- Writes:
  * data/raw/mlb_schedule_today.csv        (snapshot with ET-normalized 'date', ET-only rows)
  * data/raw/todaysgames_normalized.csv    (updated with game_id + MLB team meta)
"""

from __future__ import annotations
import os
import sys
import json
import re
import datetime as dt
from typing import Dict, Any, List
from pathlib import Path

import pandas as pd
import requests


# ------------------------------
# Paths
# ------------------------------
GAMES_CSV     = Path("data/raw/todaysgames_normalized.csv")
SCHEDULE_OUT  = Path("data/raw/mlb_schedule_today.csv")


# ------------------------------
# HTTP helper
# ------------------------------
def get(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


# ------------------------------
# Time helpers (America/New_York)
# ------------------------------
def today_et_str() -> str:
    """Return today's date in America/New_York (YYYY-MM-DD)."""
    return pd.Timestamp.now(tz="America/New_York").date().isoformat()


def utc_iso_to_et_date(series: pd.Series) -> pd.Series:
    """
    Convert MLB 'gameDate' (UTC ISO strings) to ET date strings (YYYY-MM-DD).
    """
    ts = pd.to_datetime(series, utc=True, errors="coerce")
    return ts.dt.tz_convert("America/New_York").dt.date.astype("string")


# ------------------------------
# Canonical team key mapping
# ------------------------------
_TEAM_ALIASES = {
    # Minimal canonicalizer — match what normalized games use
    "CHICAGO WHITE SOX": "whitesox",
    "CHICAGO CUBS": "chicagocubs",
    "BOSTON RED SOX": "redsox",
    "LOS ANGELES ANGELS": "losangelesangels",
    "ARIZONA DIAMONDBACKS": "arizonadiamondbacks",
    "ATLANTA BRAVES": "atlantabraves",
    "BALTIMORE ORIOLES": "baltimoreorioles",
    "BOSTON RED SOX": "bostonredsox",
    "CHICAGO CUBS": "chicagocubs",
    "CHICAGO WHITE SOX": "chicagowhitesox",
    "CINCINNATI REDS": "cincinnatireds",
    "CLEVELAND GUARDIANS": "clevelandguardians",
    "COLORADO ROCKIES": "coloradorockies",
    "DETROIT TIGERS": "detroittigers",
    "HOUSTON ASTROS": "houstonastros",
    "KANSAS CITY ROYALS": "kansascityroyals",
    "LOS ANGELES DODGERS": "losangelesdodgers",
    "MIAMI MARLINS": "miamimarlins",
    "MILWAUKEE BREWERS": "milwaukeebrewers",
    "MINNESOTA TWINS": "minnesotatwins",
    "NEW YORK METS": "newyorkmets",
    "NEW YORK YANKEES": "newyorkyankees",
    "OAKLAND ATHLETICS": "athletics",  # your preferred canonical short
    "PHILADELPHIA PHILLIES": "philadelphiaphillies",
    "PITTSBURGH PIRATES": "pittsburghpirates",
    "SAN DIEGO PADRES": "sandiegopadres",
    "SAN FRANCISCO GIANTS": "sanfranciscogiants",
    "SEATTLE MARINERS": "seattlemariners",
    "ST. LOUIS CARDINALS": "cardinals",
    "TAMPA BAY RAYS": "tampabayrays",
    "TEXAS RANGERS": "texasrangers",
    "TORONTO BLUE JAYS": "bluejays",
    "WASHINGTON NATIONALS": "washingtonnationals",
}

def canon_team_key(name: str) -> str:
    s = str(name or "").strip().upper()
    s = re.sub(r"[^A-Z0-9 ]", "", s)
    return _TEAM_ALIASES.get(s, s.replace(" ", "").lower())


# ------------------------------
# Data extraction
# ------------------------------
def extract_schedule(date_str: str) -> pd.DataFrame:
    base = "https://statsapi.mlb.com/api/v1/schedule"
    url = f"{base}?sportId=1&date={date_str}"
    data = get(url)

    rows: List[Dict[str, Any]] = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            home = g.get("teams", {}).get("home", {}).get("team", {}) or {}
            away = g.get("teams", {}).get("away", {}).get("team", {}) or {}
            status = g.get("status", {}) or {}
            venue = (g.get("venue") or {}).get("name")

            rows.append({
                "game_id": g.get("gamePk"),
                "game_datetime": g.get("gameDate"),            # UTC ISO
                "game_number": g.get("gameNumber"),
                "status_code": status.get("statusCode"),
                "home_team_name": home.get("name"),
                "home_team_abbr": home.get("abbreviation"),
                "home_team_id": home.get("id"),
                "away_team_name": away.get("name"),
                "away_team_abbr": away.get("abbreviation"),
                "away_team_id": away.get("id"),
                "venue_name": venue,
            })

    df = pd.DataFrame(rows)

    if not df.empty:
        # Add canonical keys used for merging
        df["home_key"] = df["home_team_name"].map(canon_team_key)
        df["away_key"] = df["away_team_name"].map(canon_team_key)
        # Add ET-normalized calendar date
        df["date"] = utc_iso_to_et_date(df["game_datetime"])

    return df


def load_games_input(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"ℹ️ {path} missing; nothing to enrich.", file=sys.stderr)
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ("home_team", "away_team"):
        if col not in df.columns:
            df[col] = ""
    df["home_key"] = df["home_team"].astype(str).map(canon_team_key)
    df["away_key"] = df["away_team"].astype(str).map(canon_team_key)
    return df


# ------------------------------
# Main
# ------------------------------
def main():
    # Resolve target ET 'today'
    et_today = os.environ.get("MLB_DATE", "").strip() or today_et_str()

    # Pull schedule for that (nominal) day
    sched_df = extract_schedule(et_today)

    # If nothing came back, still write an empty snapshot (with headers) and exit 0
    if sched_df.empty:
        print(f"⚠️ MLB schedule is empty for {et_today}")
        SCHEDULE_OUT.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=[
            "game_id","game_datetime","game_number","status_code",
            "home_team_name","home_team_abbr","home_team_id",
            "away_team_name","away_team_abbr","away_team_id",
            "venue_name","home_key","away_key","date"
        ]).to_csv(SCHEDULE_OUT, index=False)
        return

    # Filter strictly to ET 'today' to avoid UTC spillover rows
    sched_df = sched_df[sched_df["date"] == et_today].copy()

    # Save schedule snapshot with ET-normalized 'date'
    SCHEDULE_OUT.parent.mkdir(parents=True, exist_ok=True)
    sched_df.to_csv(SCHEDULE_OUT, index=False)
    print(f"✅ fetch_mlb_ids: wrote {len(sched_df)} rows -> {SCHEDULE_OUT} (ET date={et_today})")

    # Enrich our normalized games file with game_id + MLB team meta
    df_games = load_games_input(GAMES_CSV)
    if df_games.empty:
        print(f"ℹ️ {GAMES_CSV} missing or empty; skipping enrichment.", file=sys.stderr)
        return

    # Left-join schedule by canonical team keys; handle doubleheaders via (home_key, away_key, game_number) when available
    merged = df_games.merge(
        sched_df.drop_duplicates(),
        left_on=["home_key", "away_key"],
        right_on=["home_key", "away_key"],
        how="left",
        suffixes=("", "_sched"),
    )

    # If some fail to match due to order, try flipped match and fill where missing
    need = merged["game_id"].isna()
    if need.any():
        flip = df_games.merge(
            sched_df.drop_duplicates(),
            left_on=["away_key", "home_key"],   # flipped
            right_on=["home_key", "away_key"],
            how="left",
            suffixes=("", "_flip"),
        )
        for src, dst in [
            ("game_id", "game_id"),
            ("game_datetime", "game_datetime"),
            ("game_number", "game_number"),
            ("status_code", "status_code"),
            ("home_team_name", "home_team_name"),
            ("home_team_abbr", "home_team_abbr"),
            ("home_team_id", "home_team_id"),
            ("away_team_name", "away_team_name"),
            ("away_team_abbr", "away_team_abbr"),
            ("away_team_id", "away_team_id"),
            ("venue_name", "venue_name"),
            ("date", "date"),
        ]:
            merged[dst] = merged[dst].where(~need, flip[src])

    # Keep original games columns plus added MLB metadata
    keep_cols = list(df_games.columns)
    add_cols = [
        "game_id","game_datetime","game_number","status_code",
        "home_team_name","home_team_abbr","home_team_id",
        "away_team_name","away_team_abbr","away_team_id",
        "venue_name","date",
    ]
    out = merged[keep_cols + add_cols]

    # Write updated games file (overwrite)
    out.to_csv(GAMES_CSV, index=False)
    print(f"✅ fetch_mlb_ids: enriched {GAMES_CSV} with game_id/meta; rows={len(out)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ fetch_mlb_ids: {e}", file=sys.stderr)
        sys.exit(1)
