#!/usr/bin/env python3
"""
Normalize teams/dates for today's games using manual mappings.

Inputs
- data/raw/todaysgames.csv  (from scripts/todaysgames.py)
- data/manual/mlb_team_ids.csv  (Team Name,Team ID,Abbreviation)
- data/manual/team_name_map.csv (optional; columns: alias, canonical)

Output
- data/raw/todaysgames_normalized.csv
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

IN_FILE  = Path("data/raw/todaysgames.csv")
OUT_FILE = Path("data/raw/todaysgames_normalized.csv")
TEAM_IDS = Path("data/manual/mlb_team_ids.csv")
TEAM_MAP = Path("data/manual/team_name_map.csv")  # optional

def _norm(s: str) -> str:
    return "".join((s or "").strip().lower().replace(" ", "").replace("_",""))

def main():
    # Load base
    df = pd.read_csv(IN_FILE)
    df.columns = [c.strip() for c in df.columns]

    # Load manual team IDs (required)
    t = pd.read_csv(TEAM_IDS)
    t.columns = [c.strip() for c in t.columns]
    # Ensure expected headers
    if not {"Team Name","Team ID","Abbreviation"}.issubset(set(t.columns)):
        raise SystemExit("mlb_team_ids.csv must have: Team Name, Team ID, Abbreviation")

    # Build canonical map by abbreviation and common aliases
    # Canonical = spaced, title-cased club name (e.g., "White Sox")
    t["canonical"] = t["Team Name"].astype(str).str.strip()
    t["abbr"]      = t["Abbreviation"].astype(str).str.strip()

    # Optional alias map
    alias = {}
    if TEAM_MAP.exists():
        amap = pd.read_csv(TEAM_MAP)
        amap.columns = [c.strip() for c in amap.columns]
        if {"alias","canonical"}.issubset(amap.columns):
            for _, r in amap.iterrows():
                alias[_norm(str(r["alias"]))] = str(r["canonical"]).strip()

    # Hard normalizations that have tripped joins
    hard = {
        _norm("WhiteSox"): "White Sox",
        _norm("Athletics"): "Athletics",
        _norm("A's"): "Athletics",
        _norm("AZ"): "Arizona Diamondbacks",
        _norm("CWS"): "White Sox",   # if 3-letter used in any step
        _norm("OAK"): "Athletics",
    }

    # Build lookup from multiple keys -> canonical + abbr + team_id
    look = {}
    for _, r in t.iterrows():
        can = r["canonical"]
        ab  = r["abbr"]
        tid = int(r["Team ID"])
        for key in { _norm(can), _norm(ab) }:
            look[key] = (can, ab, tid)

    # Apply alias and hard overrides
    def canonize(token: str) -> tuple[str,str,int|None]:
        k = _norm(token)
        if k in alias:
            k = _norm(alias[k])
        if k in hard:
            k = _norm(hard[k])
        return look.get(k, (token, token, None))

    # Normalize home/away to canonical + attach IDs
    homes = df["home_team"].astype(str)
    aways = df["away_team"].astype(str)

    can_home, ab_home, id_home = [], [], []
    can_away, ab_away, id_away = [], [], []

    for v in homes:
        c,a,i = canonize(v)
        can_home.append(c); ab_home.append(a); id_home.append(i)

    for v in aways:
        c,a,i = canonize(v)
        can_away.append(c); ab_away.append(a); id_away.append(i)

    out = df.copy()
    out["home_team_canonical"] = can_home
    out["away_team_canonical"] = can_away
    out["home_team_abbr"]      = ab_home
    out["away_team_abbr"]      = ab_away
    out["home_team_id"]        = id_home
    out["away_team_id"]        = id_away

    # Ensure single, correct date column (ET)
    today_et = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    out["date"] = today_et
    # Drop any accidental duplicates like "date.1"
    out = out.loc[:, ~out.columns.duplicated()]

    # Reorder to stable schema
    cols = [
        "home_team_canonical","away_team_canonical",
        "home_team_abbr","away_team_abbr",
        "home_team_id","away_team_id",
        "game_time","pitcher_home","pitcher_away","date"
    ]
    out = out[cols]

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)

if __name__ == "__main__":
    main()
