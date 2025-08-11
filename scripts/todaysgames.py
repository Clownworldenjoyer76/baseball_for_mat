#!/usr/bin/env python3
"""
Creates data/raw/todaysgames.csv with columns:
  home_team,away_team,game_time,pitcher_home,pitcher_away
- Uses MLB Stats API schedule
- Times shown in America/New_York
- Pitchers "Last, First"
- CSV values always quoted
- Extensive debug logging
"""
import argparse, csv, json, os, sys, traceback
from datetime import datetime
from pathlib import Path
import requests
from zoneinfo import ZoneInfo

API = "https://statsapi.mlb.com/api/v1/schedule"
TEAM_ABBR_FALLBACK = {
    "Arizona Diamondbacks":"ARI","Atlanta Braves":"ATL","Baltimore Orioles":"BAL","Boston Red Sox":"BOS",
    "Chicago Cubs":"CHC","Chicago White Sox":"CHW","Cincinnati Reds":"CIN","Cleveland Guardians":"CLE",
    "Colorado Rockies":"COL","Detroit Tigers":"DET","Houston Astros":"HOU","Kansas City Royals":"KC",
    "Los Angeles Angels":"LAA","Los Angeles Dodgers":"LAD","Miami Marlins":"MIA","Milwaukee Brewers":"MIL",
    "Minnesota Twins":"MIN","New York Mets":"NYM","New York Yankees":"NYY","Oakland Athletics":"ATH","Athletics":"ATH",
    "Philadelphia Phillies":"PHI","Pittsburgh Pirates":"PIT","San Diego Padres":"SD","San Francisco Giants":"SF",
    "Seattle Mariners":"SEA","St. Louis Cardinals":"STL","Tampa Bay Rays":"TB","Texas Rangers":"TEX",
    "Toronto Blue Jays":"TOR","Washington Nationals":"WSH",
}

def debug(msg:str): print(f"DEBUG: {msg}", file=sys.stderr, flush=True)

def fmt_time_local(iso_utc:str, tz="America/New_York")->str:
    try:
        dt_utc = datetime.fromisoformat(iso_utc.replace("Z","+00:00"))
        dt_loc = dt_utc.astimezone(ZoneInfo(tz))
        try: return dt_loc.strftime("%-I:%M %p")
        except ValueError: return dt_loc.strftime("%I:%M %p").lstrip("0")
    except Exception as e:
        debug(f"time parse error '{iso_utc}': {e}"); return ""

def to_last_first(full_name:str)->str:
    if not full_name: return "Undecided"
    parts = full_name.strip().split()
    return parts[0] if len(parts)==1 else f"{parts[-1]}, {' '.join(parts[:-1])}"

def pitcher_name(pp:dict)->str:
    if not isinstance(pp, dict) or not pp: return "Undecided"
    nm = (pp.get("fullName") or "").strip()
    return to_last_first(nm) if nm else "Undecided"

def map_team_abbr(team:dict)->str:
    if not isinstance(team, dict): return ""
    ab = (team.get("abbreviation") or "").strip()
    nm = (team.get("name") or "").strip()
    chosen = "ATH" if ("Athletics" in nm or ab=="OAK") else (ab or TEAM_ABBR_FALLBACK.get(nm, nm))
    debug(f"Team map -> name='{nm}' abbr_api='{ab}' chosen='{chosen}'")
    return chosen

def fetch_games(date_str:str)->list:
    params = {"sportId":1,"date":date_str,"hydrate":"probablePitcher(note,fullName),team","language":"en"}
    debug(f"HTTP GET {API}"); debug(f"Params: {json.dumps(params, indent=2)}")
    r = requests.get(API, params=params, timeout=30)
    debug(f"Response status: {r.status_code}"); debug(f"Final URL: {r.url}")
    r.raise_for_status()
    debug(f"Response preview (500): {r.text[:500].replace(chr(10),' ')!r}")
    data = r.json()
    dates = data.get("dates", [])
    games = dates[0].get("games", []) if dates else []
    debug(f"Games found: {len(games)}")
    return games

def build_rows(games:list)->list:
    rows=[]
    for i,g in enumerate(games,1):
        home = (g.get("teams",{}).get("home") or {})
        away = (g.get("teams",{}).get("away") or {})
        home_abbr = map_team_abbr((home.get("team") or {}))
        away_abbr = map_team_abbr((away.get("team") or {}))
        game_time = fmt_time_local(g.get("gameDate",""))
        home_pp = pitcher_name(home.get("probablePitcher"))
        away_pp = pitcher_name(away.get("probablePitcher"))
        debug(f"[{i}] {away_abbr}@{home_abbr} time='{game_time}' pitchers: home='{home_pp}', away='{away_pp}'")
        rows.append({"home_team":home_abbr,"away_team":away_abbr,"game_time":game_time,
                     "pitcher_home":home_pp,"pitcher_away":away_pp})
    return rows

def ensure_parent(path:Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    debug(f"Ensured parent dir: {path.parent}")

def write_csv(rows:list, out_path:Path):
    ensure_parent(out_path)
    fns = ["home_team","away_team","game_time","pitcher_home","pitcher_away"]
    debug(f"Writing CSV -> {out_path.resolve()}"); debug(f"Row count: {len(rows)}")
    if rows[:3]: debug(f"Row preview: {json.dumps(rows[:3], indent=2)}")
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fns, quoting=csv.QUOTE_ALL)
        w.writeheader(); w.writerows(rows); f.flush(); os.fsync(f.fileno())
    size = out_path.stat().st_size
    debug(f"Wrote CSV bytes: {size}")
    try:
        with out_path.open("r", encoding="utf-8") as f: head="".join([next(f) for _ in range(5)])
        debug("CSV head:\n"+head)
    except Exception as e: debug(f"head preview error: {e}")

def main():
    p = argparse.ArgumentParser(description="Create MLB CSV (debug-heavy).")
    p.add_argument("--date", default=None, help="YYYY-MM-DD (default: today ET)")
    p.add_argument("--out",  default="data/raw/todaysgames.csv", help="Output CSV path")
    p.add_argument("--tz",   default="America/New_York", help="Display timezone")
    p.add_argument("--header-only-on-empty", action="store_true")
    a = p.parse_args()

    debug(f"Python: {sys.version}")
    debug(f"CWD: {Path.cwd().resolve()}")
    debug(f"Output target: {Path(a.out).resolve()} (exists_pre={Path(a.out).exists()})")
    debug(f"ENV TZ: {os.environ.get('TZ')} / arg tz={a.tz}")

    date_str = a.date or datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    debug(f"Target date: {date_str}")

    try:
        rows = build_rows(fetch_games(date_str))
        if not rows and a.header_only_on_empty: rows=[]
        write_csv(rows, Path(a.out))
    except requests.HTTPError as e:
        debug(f"HTTP error: {e}"); debug(traceback.format_exc()); write_csv([], Path(a.out)); sys.exit(2)
    except Exception as e:
        debug(f"Unhandled error: {e}"); debug(traceback.format_exc()); write_csv([], Path(a.out)); sys.exit(1)

    outp = Path(a.out)
    debug(f"File exists (post): {outp.exists()} size={outp.stat().st_size if outp.exists() else 0}")

if __name__ == "__main__":
    main()
