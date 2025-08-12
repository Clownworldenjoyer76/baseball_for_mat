#!/usr/bin/env python3
"""
fetch_mlb_ids.py

- Calls MLB StatsAPI schedule for a given date (default: today, ET unless MLB_DATE set).
- Extracts gamePk -> game_id and team ids/abbrs/names.
- Merges onto data/raw/todaysgames_normalized.csv using robust team-name normalization.
- Writes:
  * data/raw/mlb_schedule_today.csv  (raw schedule snapshot)
  * data/raw/todaysgames_normalized.csv (updated with game_id + MLB team meta)
"""

from __future__ import annotations
import os, sys, json, re, datetime as dt
from typing import Dict, Any, List
import pandas as pd
from pathlib import Path

# --------- Helpers ---------

def today_et_str() -> str:
    # Use US/Eastern without needing pytz
    # MLB schedule endpoint accepts YYYY-MM-DD in local; ET is fine for daily slates.
    et_offset = dt.timedelta(hours=-4)  # crude; good enough for in-season (EDT). If you need DST-accurate, wire zoneinfo.
    now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    now_et = now_utc.astimezone(dt.timezone(dt.timedelta(hours=-4)))  # EDT fallback
    return now_et.date().isoformat()

NON_ALNUM = re.compile(r'[^a-z]')

def canon_team_key(name: str) -> str:
    if not isinstance(name, str):
        return ""
    key = NON_ALNUM.sub('', name.lower())

    # Aliases to canonical keys
    aliases = {
        # AL
        "whitesox": "whitesox",
        "redsox": "redsox",
        "bluejays": "bluejays",
        "yankees": "yankees",
        "rays": "rays",
        "orioles": "orioles",
        "guardians": "guardians",
        "royals": "royals",
        "tigers": "tigers",
        "twins": "twins",
        "mariners": "mariners",
        "athletics": "athletics",
        "angels": "angels",
        "rangers": "rangers",
        "astros": "astros",

        # NL
        "mets": "mets",
        "phillies": "phillies",
        "nationals": "nationals",
        "braves": "braves",
        "marlins": "marlins",
        "cubs": "cubs",
        "cardinals": "cardinals",
        "pirates": "pirates",
        "brewers": "brewers",
        "reds": "reds",
        "giants": "giants",
        "dodgers": "dodgers",
        "padres": "padres",
        "rockies": "rockies",
        "diamondbacks": "diamondbacks",
        # Common D-backs/white-space variants
        "dbacks": "diamondbacks",
        "dbacks": "diamondbacks",
        "dback": "diamondbacks",
        "dbacksarizona": "diamondbacks",
        "arizonadiamondbacks": "diamondbacks",
        "arizona": "diamondbacks",   # only safe for schedule merge context
        "whitesoxchicago": "whitesox",
        "chicagowhitesox": "whitesox",
        "torontobluejays": "bluejays",
        "bostonredsox": "redsox",
    }
    return aliases.get(key, key)

def get(url: str) -> Dict[str, Any]:
    # Prefer requests, fall back to urllib
    try:
        import requests
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        import urllib.request
        with urllib.request.urlopen(url, timeout=20) as resp:  # type: ignore
            return json.loads(resp.read().decode("utf-8"))

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
                "game_datetime": g.get("gameDate"),
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
    # Normalize keys used for joining
    if not df.empty:
        df["home_key"] = df["home_team_name"].map(canon_team_key)
        df["away_key"] = df["away_team_name"].map(canon_team_key)
    return df

def load_games_input(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"⚠️ {path} not found. Nothing to enrich.", file=sys.stderr)
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Expect columns: home_team, away_team (post-normalization scripts already ran)
    for col in ("home_team", "away_team"):
        if col not in df.columns:
            # Try fallbacks
            for alt in ("home", "away"):
                pass
            df[col] = ""
    df["home_key"] = df["home_team"].astype(str).map(canon_team_key)
    df["away_key"] = df["away_team"].astype(str).map(canon_team_key)
    return df

def main():
    # Paths
    games_csv = Path("data/raw/todaysgames_normalized.csv")
    schedule_out = Path("data/raw/mlb_schedule_today.csv")

    # Date
    date_str = os.environ.get("MLB_DATE", "").strip() or today_et_str()

    # Pull schedule
    sched_df = extract_schedule(date_str)
    if sched_df.empty:
        print(f"⚠️ MLB schedule is empty for {date_str}")
        # Still write an empty snapshot for debug and exit 0
        schedule_out.parent.mkdir(parents=True, exist_ok=True)
        sched_df.to_csv(schedule_out, index=False)
        return

    # Save snapshot for debugging
    schedule_out.parent.mkdir(parents=True, exist_ok=True)
    sched_df.to_csv(schedule_out, index=False)

    # Load our normalized games
    df = load_games_input(games_csv)
    if df.empty:
        print("⚠️ No games to enrich. Exiting cleanly.")
        return

    # Merge on normalized home/away keys
    merged = df.merge(
        sched_df[
            [
                "home_key","away_key",
                "game_id","game_datetime","game_number","status_code",
                "home_team_name","home_team_abbr","home_team_id",
                "away_team_name","away_team_abbr","away_team_id",
                "venue_name",
            ]
        ],
        on=["home_key","away_key"],
        how="left",
        suffixes=("","_mlb"),
    )

    matched = merged["game_id"].notna().sum()
    total = len(merged)
    print(f"🧾 fetch_mlb_ids: matched {matched}/{total} games by team keys.")

    # For any unmatched, try a secondary pass swapping keys (in case upstream home/away flipped)
    if matched < total:
        remaining = merged[merged["game_id"].isna()][["home_key","away_key"]].drop_duplicates()
        if not remaining.empty:
            flip = df.merge(
                sched_df[
                    [
                        "home_key","away_key",
                        "game_id","game_datetime","game_number","status_code",
                        "home_team_name","home_team_abbr","home_team_id",
                        "away_team_name","away_team_abbr","away_team_id",
                        "venue_name",
                    ]
                ],
                left_on=["home_key","away_key"],
                right_on=["away_key","home_key"],  # flipped
                how="left",
                suffixes=("","_flip"),
            )
            # Fill only where missing
            for col_src, col_dst in [
                ("game_id","game_id"),
                ("game_datetime","game_datetime"),
                ("game_number","game_number"),
                ("status_code","status_code"),
                ("home_team_name","home_team_name"),
                ("home_team_abbr","home_team_abbr"),
                ("home_team_id","home_team_id"),
                ("away_team_name","away_team_name"),
                ("away_team_abbr","away_team_abbr"),
                ("away_team_id","away_team_id"),
                ("venue_name","venue_name"),
            ]:
                merged[col_dst] = merged[col_dst].fillna(flip[col_src])

            matched2 = merged["game_id"].notna().sum()
            if matched2 > matched:
                print(f"🔁 Secondary flip pass added {matched2 - matched} matches (now {matched2}/{total}).")
                matched = matched2

    # Report any still-unmatched
    if matched < total:
        um = merged.loc[merged["game_id"].isna(), ["home_team","away_team"]]
        print("⚠️ Unmatched games after schedule merge:", file=sys.stderr)
        print(um.to_string(index=False), file=sys.stderr)

    # Write updated games file
    keep_cols = list(df.columns)
    add_cols = [
        "game_id","game_datetime","game_number","status_code",
        "home_team_name","home_team_abbr","home_team_id",
        "away_team_name","away_team_abbr","away_team_id",
        "venue_name",
    ]
    out = merged[keep_cols + add_cols]
    out.to_csv(games_csv, index=False)
    print(f"✅ fetch_mlb_ids: enriched {games_csv} with game_id and MLB metadata.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Don’t crash the whole pipeline; print, then non-zero to make failure visible in STATUS if desired.
        print(f"❌ fetch_mlb_ids: {e}", file=sys.stderr)
        sys.exit(1)
