#!/usr/bin/env python3
"""
scripts/todaysgames.py
Creates todaysgames.csv with columns:
  home_team, away_team, game_time, pitcher_home, pitcher_away
- Uses MLB Stats API schedule
- Times shown in America/New_York
- Pitchers formatted "Last, First"
- CSV values are always quoted
- EXTENSIVE DEBUG LOGGING
"""

import argparse
import csv
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import requests
from zoneinfo import ZoneInfo

API = "https://statsapi.mlb.com/api/v1/schedule"

# Optional fallback mapping if API doesn't provide abbreviations
TEAM_ABBR_FALLBACK = {
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
    # User prefers ATH instead of OAK
    "Oakland Athletics": "ATH",
    "Athletics": "ATH",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

def debug(msg: str):
    print(f"DEBUG: {msg}", file=sys.stderr, flush=True)

def fmt_time_local(iso_utc: str, tz: str = "America/New_York") -> str:
    """Convert MLB ISO time (UTC) to local time string like '6:10 PM' (no leading zero)."""
    try:
        dt_utc = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        dt_loc = dt_utc.astimezone(ZoneInfo(tz))
        try:
            return dt_loc.strftime("%-I:%M %p")  # Linux/macOS
        except ValueError:
            return dt_loc.strftime("%I:%M %p").lstrip("0")  # Windows fallback
    except Exception as e:
        debug(f"Time parse error for '{iso_utc}': {e}")
        return ""

def to_last_first(full_name: str) -> str:
    """Format 'First Middle Last Jr.' -> 'Last, First Middle Jr.'"""
    if not full_name:
        return "Undecided"
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0]
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

def pitcher_name(prob_pitcher: dict) -> str:
    if not isinstance(prob_pitcher, dict) or not prob_pitcher:
        return "Undecided"
    # Stats API hydrate should include fullName
    name = (prob_pitcher.get("fullName") or "").strip()
    return to_last_first(name) if name else "Undecided"

def map_team_abbr(team_obj: dict) -> str:
    """Prefer API abbreviation; override A's to ATH; fallback to name mapping."""
    if not isinstance(team_obj, dict):
        return ""
    abbr = (team_obj.get("abbreviation") or "").strip()
    name = (team_obj.get("name") or "").strip()
    # Special-case Athletics preference
    if "Athletics" in name or abbr == "OAK":
        chosen = "ATH"
    elif abbr:
        chosen = abbr
    else:
        chosen = TEAM_ABBR_FALLBACK.get(name, name)
    debug(f"Team map -> name='{name}' abbr_api='{abbr}' chosen='{chosen}'")
    return chosen

def fetch_games(date_str: str) -> list:
    params = {
        "sportId": 1,
        "date": date_str,
        "hydrate": "probablePitcher(note,fullName),team",
        "language": "en",
    }
    debug(f"HTTP GET {API}")
    debug(f"Params: {json.dumps(params, indent=2)}")
    r = requests.get(API, params=params, timeout=30)
    debug(f"Response status: {r.status_code}")
    debug(f"Final URL: {r.url}")
    r.raise_for_status()

    text_preview = r.text[:500].replace("\n", " ")
    debug(f"Response preview (first 500 chars): {text_preview!r}")
    try:
        data = r.json()
    except Exception:
        debug("JSON decode failed; raising...")
        raise

    dates = data.get("dates", [])
    if not dates:
        debug("No 'dates' in response (zero games or different key).")
        return []
    games = dates[0].get("games", [])
    debug(f"Games found: {len(games)}")
    return games

def build_rows(games: list) -> list:
    rows = []
    for i, g in enumerate(games, 1):
        teams = g.get("teams", {})
        home = teams.get("home", {}) or {}
        away = teams.get("away", {}) or {}

        home_team_obj = (home.get("team") or {})
        away_team_obj = (away.get("team") or {})

        home_abbr = map_team_abbr(home_team_obj)
        away_abbr = map_team_abbr(away_team_obj)

        home_pp = pitcher_name(home.get("probablePitcher"))
        away_pp = pitcher_name(away.get("probablePitcher"))

        game_date_iso = g.get("gameDate", "")
        game_time = fmt_time_local(game_date_iso)

        debug(
            f"[{i}] {away_abbr}@{home_abbr} time_iso='{game_date_iso}' -> '{game_time}' "
            f"pitchers: home='{home_pp}', away='{away_pp}'"
        )

        rows.append({
            "home_team": home_abbr,
            "away_team": away_abbr,
            "game_time": game_time or "",
            "pitcher_home": home_pp,
            "pitcher_away": away_pp,
        })
    return rows

def ensure_parent_dir(path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        debug(f"Ensured parent directory exists: {path.parent}")
    except Exception as e:
        debug(f"Failed to create parent directory '{path.parent}': {e}")
        raise

def write_csv(rows: list, out_path: Path):
    ensure_parent_dir(out_path)
    fieldnames = ["home_team", "away_team", "game_time", "pitcher_home", "pitcher_away"]
    debug(f"Writing CSV -> {out_path.resolve()}")
    debug(f"Row count: {len(rows)}")
    if rows[:3]:
        debug(f"Row preview (first up to 3): {json.dumps(rows[:3], indent=2)}")

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[h.strip() for h in fieldnames], quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)
        f.flush()
        os.fsync(f.fileno())

    size = out_path.stat().st_size
    debug(f"Wrote CSV bytes: {size}")
    # Show first few lines
    try:
        with out_path.open("r", encoding="utf-8") as f:
            head = "".join([next(f) for _ in range(5)])
        debug("CSV head:\n" + head)
    except Exception as e:
        debug(f"Failed to preview CSV head: {e}")

def main():
    parser = argparse.ArgumentParser(description="Create MLB CSV with specific columns (debug-heavy).")
    parser.add_argument("--date", help="YYYY-MM-DD (default: today in America/New_York)", default=None)
    parser.add_argument("--out", help="Output CSV path", default="todaysgames.csv")
    parser.add_argument("--tz", help="Timezone for display (IANA name)", default="America/New_York")
    parser.add_argument("--header-only-on-empty", action="store_true",
                        help="If no games, still create file with just the header.")
    args = parser.parse_args()

    # Environment and context debug
    debug(f"Python: {sys.version}")
    debug(f"CWD: {Path.cwd().resolve()}")
    debug(f"File exists (pre): {Path(args.out).exists()} -> {Path(args.out).resolve()}")
    debug(f"ENV[GITHUB_REF_NAME]: {os.environ.get('GITHUB_REF_NAME')}")
    debug(f"ENV[GITHUB_SHA]: {os.environ.get('GITHUB_SHA')}")
    debug(f"ENV[TZ]: {os.environ.get('TZ')} (arg tz={args.tz})")

    # Determine date (ET by default)
    if args.date:
        date_str = args.date
    else:
        date_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    debug(f"Target date: {date_str}")

    try:
        games = fetch_games(date_str)
        rows = build_rows(games)
        if not rows and args.header_only_on_empty:
            debug("No games returned; writing header-only CSV as requested.")
            rows = []
        write_csv(rows, Path(args.out))
    except requests.HTTPError as http_err:
        debug(f"HTTP error: {http_err}")
        debug(traceback.format_exc())
        # Still produce header-only file so CI can commit something
        try:
            write_csv([], Path(args.out))
        except Exception:
            pass
        sys.exit(2)
    except Exception as e:
        debug(f"Unhandled error: {e}")
        debug(traceback.format_exc())
        # Still attempt to create header-only file
        try:
            write_csv([], Path(args.out))
        except Exception:
            pass
        sys.exit(1)

    # Post conditions
    outp = Path(args.out)
    exists = outp.exists()
    size = outp.stat().st_size if exists else 0
    debug(f"File exists (post): {exists} size={size}")
    if not exists or size == 0:
        debug("WARNING: Output file missing or empty after write attempt.")

if __name__ == "__main__":
    main()
