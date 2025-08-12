#!/usr/bin/env python3
import argparse, json, os, sys, time, csv
from datetime import datetime, date
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import pandas as pd

GAMES_IN   = Path("data/raw/todaysgames_normalized.csv")
OUT_CSV    = Path("data/raw/mlb_game_ids.csv")
CACHE_DIR  = Path("data/cache")
SPORT_ID   = 1  # MLB
API_TMPL   = "https://statsapi.mlb.com/api/v1/schedule?sportId={sport}&date={d}"

# Canonical keys (lowercase, alnum only) → MLB abbrev
TEAM_KEY_TO_ABBR = {
    "yankees":"NYY","redsox":"BOS","whitesox":"CWS","guardians":"CLE","tigers":"DET","royals":"KC",
    "twins":"MIN","orioles":"BAL","bluejays":"TOR","rays":"TB","mariners":"SEA","angels":"LAA",
    "rangers":"TEX","athletics":"OAK","astros":"HOU",
    "mets":"NYM","phillies":"PHI","braves":"ATL","marlins":"MIA","nationals":"WSH",
    "cubs":"CHC","cardinals":"STL","pirates":"PIT","reds":"CIN","brewers":"MIL",
    "dodgers":"LAD","giants":"SF","padres":"SD","rockies":"COL",
    # D-backs variations
    "diamondbacks":"ARI","dbacks":"ARI","dback":"ARI","dbacksarizona":"ARI","arizonadiamondbacks":"ARI",
    # White Sox variations
    "whitesox":"CWS","white Sox":"CWS","chisox":"CWS",
    # Red Sox variation
    "redsox":"BOS",
}

# Extra aliasing for inputs we’ve seen in your logs
INPUT_ALIAS = {
    "az":"diamondbacks", "cws":"whitesox", "white sox":"whitesox", "red sox":"redsox",
}

def _key(s: str) -> str:
    if s is None:
        return ""
    k = "".join(ch for ch in str(s).lower() if ch.isalnum())
    if k in INPUT_ALIAS:  # map early
        k = INPUT_ALIAS[k]
    return k

def _abbr_for(team_text: str) -> str | None:
    k = _key(team_text)
    return TEAM_KEY_TO_ABBR.get(k)

def _mlb_schedule(day: str) -> dict:
    url = API_TMPL.format(sport=SPORT_ID, d=day)
    for i in range(5):
        try:
            req = Request(url, headers={"User-Agent": "mlb-game-id-fetcher/1.0"})
            with urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode("utf-8"))
        except (URLError, HTTPError) as e:
            time.sleep(1.5 * (i+1))
            last = e
    raise SystemExit(f"❌ Failed to fetch MLB schedule after retries: {last}")

def _games_from_schedule(payload: dict) -> list[dict]:
    games = []
    for d in payload.get("dates", []):
        for g in d.get("games", []):
            try:
                games.append({
                    "game_pk": g["gamePk"],
                    "game_datetime": g.get("gameDate"),
                    "game_number": g.get("gameNumber"),
                    "home_abbr": g["teams"]["home"]["team"]["abbreviation"],
                    "away_abbr": g["teams"]["away"]["team"]["abbreviation"],
                    "status": g.get("status", {}).get("detailedState"),
                })
            except KeyError:
                continue
    return games

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (defaults to today in ET)", default=None)
    ap.add_argument("--games", help="Input path to todaysgames_normalized.csv", default=str(GAMES_IN))
    ap.add_argument("--out", help="Output CSV path", default=str(OUT_CSV))
    args = ap.parse_args()

    day = args.date or date.today().isoformat()

    games_path = Path(args.games)
    if not games_path.exists():
        print(f"❌ games file not found: {games_path}")
        sys.exit(0)

    df = pd.read_csv(games_path)
    # Expect columns home_team, away_team (already normalized by earlier steps)
    needed = {"home_team","away_team"}
    if not needed.issubset(df.columns):
        print(f"❌ {games_path} missing required columns {sorted(needed - set(df.columns))}")
        sys.exit(0)

    # make canonical join keys
    df = df.copy()
    df["home_key"] = df["home_team"].apply(_key)
    df["away_key"] = df["away_team"].apply(_key)
    df["home_abbr"] = df["home_key"].apply(_abbr_for)
    df["away_abbr"] = df["away_key"].apply(_abbr_for)

    missing = df[(df["home_abbr"].isna()) | (df["away_abbr"].isna())]
    if not missing.empty:
        print("⚠️ Unmapped teams in input (will skip):")
        print(missing[["home_team","away_team"]].drop_duplicates().to_string(index=False))

    # pull schedule with caching
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"mlb_schedule_{day}.json"
    if cache_file.exists():
        payload = json.loads(cache_file.read_text(encoding="utf-8"))
    else:
        payload = _mlb_schedule(day)
        cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    sched = _games_from_schedule(payload)
    sch_df = pd.DataFrame(sched)
    if sch_df.empty:
        print("⚠️ MLB schedule is empty for", day)

    # join by MLB abbreviations
    merged = df.merge(
        sch_df,
        left_on=["home_abbr","away_abbr"],
        right_on=["home_abbr","away_abbr"],
        how="left",
        suffixes=("","_mlb")
    )

    out_cols = [
        "date","home_team","away_team",
        "home_abbr","away_abbr",
        "game_pk","game_number","game_datetime","status"
    ]
    merged["date"] = day
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    merged[out_cols].to_csv(OUT_CSV, index=False)
    print(f"✅ Wrote MLB game IDs → {OUT_CSV} (rows={len(merged)})")

if __name__ == "__main__":
    main()
