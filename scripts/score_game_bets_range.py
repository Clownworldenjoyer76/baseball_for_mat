import os, time
print(f"RUNNING SCRIPT: {__file__}")
print(f"LAST MODIFIED: {time.ctime(os.path.getmtime(__file__))}")

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
    out_path = Path(args.out)
    out_dir = out_path.parent
    if not out_dir.is_dir():
        print(f"ERROR: Output dir does not exist: {out_dir}")
        sys.exit(1)
    if not os.access(out_dir, os.W_OK):
        print(f"ERROR: Cannot write to output dir: {out_dir}")
        sys.exit(1)

    mapping = build_team_mapping()
    print("DEBUG: 'athletics' in mapping? ->", "athletics" in mapping)
    print("DEBUG: mapping['athletics'] ->", mapping.get("athletics"))

    try:
        df_bets = pd.read_csv(args.out, keep_default_na=False)
    except FileNotFoundError:
        print(f"Error: Bet file not found at '{args.out}'.", file=sys.stderr)
        sys.exit(1)

    try:
        home_col, away_col = find_team_columns(df_bets)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    for col in ["home_score", "away_score", "game_found"]:
        if col not in df_bets.columns:
            df_bets[col] = pd.NA

    try:
        url = f"{args.api}/schedule"
        params = {"sportId": 1, "date": args.date, "hydrate": "linescore,teams"}
        schedule_data = _get(url, params)
        dates = schedule_data.get("dates", [])
        if not dates:
            print(f"No games found for {args.date}.", file=sys.stderr)
            return

        games_to_score = []
        for game in dates[0]["games"]:
            home_team = game["teams"]["home"]["team"]["name"]
            away_team = game["teams"]["away"]["team"]["name"]
            home_team_api = normalize_for_match(home_team, mapping)
            away_team_api = normalize_for_match(away_team, mapping)
            games_to_score.append({
                "gamePk": game["gamePk"],
                "home_team_api": home_team_api,
                "away_team_api": away_team_api,
                "home_score": game["linescore"]["teams"]["home"].get("runs"),
                "away_score": game["linescore"]["teams"]["away"].get("runs"),
            })
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from MLB API: {e}", file=sys.stderr)
        sys.exit(1)

    matched_bets_indices = []
    print(f"\nScoring {len(df_bets)} bets from '{args.out}' against {len(games_to_score)} games for {args.date}...")
    for i, row in df_bets.iterrows():
        home_team_bet = normalize_for_match(row.get(home_col, ""), mapping)
        away_team_bet = normalize_for_match(row.get(away_col, ""), mapping)
        if args.debug:
            print(f"DEBUG: Processing bet for: '{home_team_bet}' vs '{away_team_bet}'")
        best_match = None
        best_match_score = 0
        for game in games_to_score:
            game_home = game['home_team_api']
            game_away = game['away_team_api']
            current_score = 0
            if (home_team_bet == game_home and away_team_bet == game_away) or \
               (home_team_bet == game_away and away_team_bet == game_home):
                current_score = 2
            elif (home_team_bet == game_home or home_team_bet == game_away) or \
                 (away_team_bet == game_home or away_team_bet == game_away):
                current_score = 1
            if current_score > best_match_score:
                best_match_score = current_score
                best_match = game
        if best_match and i not in matched_bets_indices:
            if pd.isna(df_bets.loc[i, "home_score"]):
                df_bets.loc[i, "home_score"] = best_match["home_score"]
            if pd.isna(df_bets.loc[i, "away_score"]):
                df_bets.loc[i, "away_score"] = best_match["away_score"]
            if pd.isna(df_bets.loc[i, "game_found"]):
                df_bets.loc[i, "game_found"] = True
            matched_bets_indices.append(i)

    unmatched_indices = [i for i in df_bets.index if i not in matched_bets_indices]
    for i in unmatched_indices:
        if pd.isna(df_bets.loc[i, "game_found"]):
            df_bets.loc[i, "game_found"] = False

    df_bets.to_csv(args.out, index=False)
    print("\n--- Process complete ---")
    print(f"Updated {len(matched_bets_indices)} bets and saved to '{args.out}'.")

if __name__ == "__main__":
    main()
