#!/usr/bin/env python3
import argparse, csv, sys, time
from pathlib import Path
from typing import Dict, Tuple
import requests
import pandas as pd

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # used if present

# Built‑in fallback mapping: common short names/aliases -> MLB StatsAPI full team names
SHORT_TO_API = {
    # AL East
    "yankees": "New York Yankees",
    "red sox": "Boston Red Sox",
    "blue jays": "Toronto Blue Jays",
    "rays": "Tampa Bay Rays",
    "orioles": "Baltimore Orioles",
    # AL Central
    "guardians": "Cleveland Guardians",
    "tigers": "Detroit Tigers",
    "twins": "Minnesota Twins",
    "royals": "Kansas City Royals",
    "white sox": "Chicago White Sox",
    # AL West
    "astros": "Houston Astros",
    "mariners": "Seattle Mariners",
    "rangers": "Texas Rangers",
    "angels": "Los Angeles Angels",
    "athletics": "Oakland Athletics",
    "a's": "Oakland Athletics",
    "as": "Oakland Athletics",
    # NL East
    "braves": "Atlanta Braves",
    "marlins": "Miami Marlins",
    "mets": "New York Mets",
    "phillies": "Philadelphia Phillies",
    "nationals": "Washington Nationals",
    # NL Central
    "cubs": "Chicago Cubs",
    "cardinals": "St. Louis Cardinals",
    "brewers": "Milwaukee Brewers",
    "reds": "Cincinnati Reds",
    "pirates": "Pittsburgh Pirates",
    # NL West
    "dodgers": "Los Angeles Dodgers",
    "giants": "San Francisco Giants",
    "padres": "San Diego Padres",
    "diamondbacks": "Arizona Diamondbacks",
    "dbacks": "Arizona Diamondbacks",
    "d-backs": "Arizona Diamondbacks",
    "rockies": "Colorado Rockies",
}

def parse_args():
    p = argparse.ArgumentParser(
        description="Score daily GAME bets: fill scores if missing, then write actual_real_run_total, run_total_diff, favorite_correct into the per-day CSV."
    )
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--api", default="https://statsapi.mlb.com/api/v1")
    p.add_argument("--out", required=True, help="Per-day game props CSV to update")
    return p.parse_args()

def _get(url, params=None, tries=3, sleep=0.8):
    for _ in range(tries):
        r = requests.get(url, params=params, timeout=25)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()

def normalize_one_team(val: str, mapping: Dict[str, str]) -> str:
    s = str(val or "").strip()
    low = s.lower()
    # if it's already a full name, keep it
    for full in mapping.values():
        if s == full:
            return s
    # nickname → full
    if low in mapping:
        return mapping[low]
    return s  # unchanged if unknown

def build_team_mapping() -> Dict[str, str]:
    # Start with fallback mapping
    mapping = SHORT_TO_API.copy()
    # If CSV map exists, augment/override
    if TEAM_MAP_FILE.exists():
        try:
            tm = pd.read_csv(TEAM_MAP_FILE)
            # Accept flexible column names
            cols = {c.lower().strip(): c for c in tm.columns}
            short_col = cols.get("team_name_short") or cols.get("short") or cols.get("nickname") or cols.get("team_short")
            api_col   = cols.get("team_name_api")   or cols.get("api")   or cols.get("full")      or cols.get("team_full")
            if short_col and api_col:
                tm["_short"] = tm[short_col].astype(str).str.strip().str.lower()
                tm["_
