#!/usr/bin/env python3
import argparse, csv, sys, time, re
from pathlib import Path
from typing import Dict, Tuple, List, Any
import requests
import pandas as pd
import numpy as np
import os

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # optional external map

TEAM_ALIASES: Dict[str, str] = {
    # AL EAST
    "new york yankees":"nyyankees","yankees":"nyyankees","nyy":"nyyankees","ny yankees":"nyyankees","yanks":"nyyankees",
    "boston red sox":"bosredsox","red sox":"bosredsox","bos":"bosredsox","bo sox":"bosredsox","bo-sox":"bosredsox",
    "toronto blue jays":"torbluejays","blue jays":"torbluejays","jays":"torbluejays","tor":"torbluejays",
    "tampa bay rays":"tampabaysrays","rays":"tampabaysrays","tb":"tampabaysrays","tbr":"tampabaysrays","devil rays":"tampabaysrays",
    "baltimore orioles":"balorioles","orioles": "balorioles","o's":"balorioles","os":"balorioles","bal":"balorioles",
    # AL CENTRAL
    "cleveland guardians":"cleguardians","guardians":"cleguardians","cle":"cleguardians","indians":"cleguardians",
    "detroit tigers":"dettigers","tigers":"dettigers","det":"dettigers",
    "minnesota twins":"mintwins","twins":"mintwins","min":"mintwins","twinkies":"mintwins",
    "kansas city royals":"kcroyals","royals":"kcroyals","kc":"kcroyals","kcr":"kcroyals",
    "chicago white sox":"chiwhitesox","white sox":"chiwhitesox","whitesox":"chiwhitesox","chisox":"chiwhitesox","cws":"chiwhitesox","chw":"chiwhitesox",
    # AL WEST
    "houston astros":"houastros","astros":"houastros","hou":"houastros","stros":"houastros",
    "seattle mariners":"seamariners","mariners":"seamariners","sea":"seamariners","m's":"seamariners","ms":"seamariners",
    "texas rangers":"texrangers","rangers":"texrangers","tex":"texrangers",
    "los angeles angels":"laangels","angels":"laangels","laa":"laangels","ana":"laangels","halos":"laangels",
    # Athletics mappings
    "oakland athletics": "oakathletics","athletics": "oakathletics","oak":"oakathletics","a's":"oakathletics","as":"oakathletics","a s":"oakathletics",
    # NL EAST
    "atlanta braves":"atlbraves","braves":"atlbraves","atl":"atlbraves",
    "miami marlins":"miamarlins","marlins":"miamarlins","mia":"miamarlins","fish":"miamarlins",
    "new york mets":"nymets","mets":"nymets","nym":"nymets","metropolitans":"nymets",
    "philadelphia phillies":"phiphillies","phillies":"phiphillies","phils":"phiphillies","phi":"phiphillies",
    "washington nationals":"wasnats","nationals":"wasnats","nats":"wasnats","was":"wasnats","wsh":"wasnats",
    # NL CENTRAL
    "chicago cubs":"chicubs","cubs":"chicubs","chc":"chicubs","cubbies":"chicubs",
    "st. louis cardinals":"stlcardinals","st louis cardinals":"stlcardinals","cardinals":"stlcardinals","stl":"stlcardinals","redbirds":"stlcardinals",
    "milwaukee brewers":"milbrewers","brewers":"milbrewers","mil":"milbrewers","brew crew":"milbrewers",
    "cincinnati reds":"cinreds","reds":"cinreds","cin":"cinreds","big red machine":"cinreds",
    "pittsburgh pirates":"pitpirates","pirates":"pitpirates","pit":"pitpirates","bucs":"pitpirates","buccos":"pitpirates",
    # NL WEST
    "los angeles dodgers":"ladodgers","la dodgers":"ladodgers","dodgers":"ladodgers","lad":"ladodgers",
    "san francisco giants":"sfgiants","giants":"sfgiants","sf":"sfgiants","sfg":"sfgiants",
    "san diego padres":"sdpadres","padres":"sdpadres","sd":"sdpadres","friars":"sdpadres",
    "arizona diamondbacks":"aridbacks","diamondbacks":"aridbacks","dbacks":"aridbacks","d-backs":"aridbacks","ari":"aridbacks","snakes":"aridbacks",
    "colorado rockies":"colrockies","rockies":"colrockies","col":"colrockies","rox":"colrockies",
}

_PUNCT_RE = re.compile(r"[.\u2019'’`-]")

def parse_args():
    p = argparse.ArgumentParser(description="Score daily GAME bets and compute run_total_diff & favorite_correct.")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--api", default="https://statsapi.mlb.com/api/v1")
    p.add_argument("--out", required=True, help="Per-day game props CSV to update")
    p.add_argument("--debug", action="store_true", help="Print match diagnostics and write unmatched report")
    return p.parse_args()

def _get(url: str, params: Dict[str, str] | None = None, tries: int = 3, sleep: float = 0.8) -> Dict[str, Any]:
    for _ in range(tries):
        r = requests.get(url, params=params, timeout=25)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()
    return {}

def _canon_key(s: str) -> str:
    s = (s or "").strip().lower()
    return " ".join(s.split())

def _loose_key(s: str) -> str:
    return _PUNCT_RE.sub("", _canon_key(s))

def build_team_mapping() -> Dict[str, str]:
    mapping = { _loose_key(k): v for k, v in TEAM_ALIASES.items() }

    canonical_keys_map = {
        "New York Yankees": "nyyankees", "Boston Red Sox": "bosredsox", "Toronto Blue Jays": "torbluejays", "Tampa Bay Rays": "tampabaysrays", "Baltimore Orioles": "balorioles",
        "Cleveland Guardians": "cleguardians", "Detroit Tigers": "dettigers", "Minnesota Twins": "mintwins", "Kansas City Royals": "kcroyals", "Chicago White Sox": "chiwhitesox",
        "Houston Astros": "houastros", "Seattle Mariners": "seamariners", "Texas Rangers": "texrangers", "Los Angeles Angels": "laangels", "Athletics": "oakathletics",
        "Atlanta Braves": "atlbraves", "Miami Marlins": "miamarlins", "New York Mets": "nymets", "Philadelphia Phillies": "phiphillies", "Washington Nationals": "wasnats",
        "Chicago Cubs": "chicubs", "St. Louis Cardinals": "stlcardinals", "Milwaukee Brewers": "milbrewers", "Cincinnati Reds": "cinreds", "Pittsburgh Pirates": "pitpirates",
        "Los Angeles Dodgers": "ladodgers", "San Francisco Giants": "sfgiants", "San Diego Padres": "sdpadres", "Arizona Diamondbacks": "aridbacks", "Colorado Rockies": "colrockies",
    }
    for full_name, canonical_key in canonical_keys_map.items():
        mapping[_loose_key(full_name)] = canonical_key

    if TEAM_MAP_FILE.exists():
        try:
            tm = pd.read_csv(TEAM_MAP_FILE)
            cols = {c.lower().strip(): c for c in tm.columns}
            short_col = cols.get("team_name_short") or cols.get("short") or cols.get("nickname") or cols.get("team_short")
            api_col   = cols.get("team_name_api")   or cols.get("api")   or cols.get("full")      or cols.get("team_full")
            if short_col and api_col:
                tm["_short"] = tm[short_col].astype(str).str.strip().str.lower()
                tm["_api"]   = tm[api_col].astype(str).str.strip()
                for s, a in zip(tm["_short"], tm["_api"]):
                    if a in canonical_keys_map:
                        mapping[_loose_key(s)] = canonical_keys_map[a]
        except Exception as e:
            print(f"⚠️ team_name_master.csv problem: {e}", file=sys.stderr)

    # Guarantee Athletics mappings always exist
    mapping[_loose_key("athletics")] = "oakathletics"
    mapping[_loose_key("oakland athletics")] = "oakathletics"
    # Guarantee Orioles mappings always exist
    mapping[_loose_key("orioles")] = "balorioles"
    mapping[_loose_key("baltimore orioles")] = "balorioles"

    return mapping

def normalize_for_match(val: str, mapping: Dict[str,str]) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    k = _loose_key(s)
    if k in mapping:
        return mapping[k]
    return k

def find_team_columns(df: pd.DataFrame) -> Tuple[str, str]:
    cols = [c.lower().strip() for c in df.columns]
    home_cols = ['home', 'home_team', 'hometeam']
    away_cols = ['away', 'away_team', 'awayteam', 'visitor', 'visitorteam']
    home_col = next((c for c in home_cols if c in cols), None)
    away_col = next((c for c in away_cols if c in cols), None)
    if not home_col or not away_col:
        raise ValueError("Could not find suitable home/away team columns in the CSV.")
    return home_col, away_col

def main():
    args = parse_args()
    mapping = build_team_mapping()
    print("DEBUG: 'athletics' in mapping? ->", "athletics" in mapping)
    print("DEBUG: mapping['athletics'] ->", mapping.get("athletics"))
    print("DEBUG: 'orioles' in mapping? ->", "orioles" in mapping)
    print("DEBUG: mapping['orioles'] ->", mapping.get("orioles"))
    print("DEBUG: 'baltimore orioles' in mapping? ->", "baltimore orioles" in mapping)
    print("DEBUG: mapping['baltimore orioles'] ->", mapping.get("baltimore orioles"))

if __name__ == "__main__":
    main()
