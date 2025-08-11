#!/usr/bin/env python3
import argparse
import csv
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import sys

API = "https://statsapi.mlb.com/api/v1/schedule"

# Map full team names from API to abbreviations in your example
TEAM_ABBR = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CHW",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "ATH",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH"
}

def fetch_games(date_str: str):
    params = {
        "sportId": 1,
        "date": date_str,
        "hydrate": "probablePitcher(note,fullName),team",
        "language": "en",
    }
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    dates = data.get("dates", [])
    return dates[0]["games"] if dates else []

def to_eastern_time(iso_utc: str) -> str:
    dt_utc = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
    dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
    return dt_et.strftime("%-I:%M %p")

def format_name(full_name: str) -> str:
    if not full_name:
        return "Undecided"
    parts = full_name.strip().split()
    if len(parts) == 1:
        return full_name.strip()
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

def get_pitcher(pp: dict) -> str:
    if not pp:
        return "Undecided"
    return format_name(pp.get("fullName", ""))

def build_rows(games):
    rows = []
    for g in games:
        teams = g.get("teams", {})
        home = teams.get("home", {})
        away = teams.get("away", {})

        home_name = (home.get("team") or {}).get("name", "").strip()
        away_name = (away.get("team") or {}).get("name", "").strip()

        home_abbr = TEAM_ABBR.get(home_name, home_name)
        away_abbr = TEAM_ABBR.get(away_name, away_name)

        home_pp = get_pitcher(home.get("probablePitcher"))
        away_pp = get_pitcher(away.get("probablePitcher"))

        game_time = to_eastern_time(g.get("gameDate"))

        rows.append({
            "home_team": home_abbr,
            "away_team": away_abbr,
            "game_time": game_time,
            "pitcher_home": home_pp,
            "pitcher_away": away_pp,
        })
    return rows

def main():
    parser = argparse.ArgumentParser(description="Create MLB CSV with specific columns.")
    parser.add_argument("--date", help="Date in YYYY-MM-DD (default: today ET)", default=None)
    parser.add_argument("--out", help="Output CSV path", default="todaysgames.csv")
    args = parser.parse_args()

    if args.date:
        date_str = args.date
    else:
        date_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    try:
        games = fetch_games(date_str)
    except Exception as e:
        print(f"Error fetching schedule: {e}", file=sys.stderr)
        sys.exit(1)

    rows = build_rows(games)

    # Clean headers (strip whitespace)
    fieldnames = [h.strip() for h in ["home_team", "away_team", "game_time", "pitcher_home", "pitcher_away"]]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {args.out} for {date_str}")

if __name__ == "__main__":
    main()
