#!/usr/bin/env python3
import argparse, sys, time, re
from pathlib import Path
from typing import Dict, Tuple, List, Any
import requests
import pandas as pd
import os

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # optional external map

TEAM_ALIASES: Dict[str, str] = {
    # AL EAST
    "new york yankees":"nyyankees","yankees":"nyyankees","nyy":"nyyankees","ny yankees":"nyyankees","yanks":"nyyankees",
    "boston red sox":"bosredsox","red sox":"bosredsox","bos":"bosredsox","bo sox":"bosredsox","bo-sox":"bosredsox",
    "toronto blue jays":"torbluejays","blue jays":"torbluejays","jays":"torbluejays","tor":"torbluejays",
    "tampa bay rays":"tampabaysrays","rays":"tampabaysrays","tb":"tampabaysrays","tbr":"tampabaysrays","devil rays":"tampabaysrays",
    "baltimore orioles":"balorioles","orioles":"balorioles","o's":"balorioles","os":"balorioles","bal":"balorioles",
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
    "oakland athletics":"oakathletics","athletics":"oakathletics","oak":"oakathletics","a's":"oakathletics","as":"oakathletics","a s":"oakathletics",
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
    p = argparse.ArgumentParser(description="Score daily GAME bets and update specific columns only.")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--api", default="https://statsapi.mlb.com/api/v1")
    p.add_argument("--out", required=True, help="Per-day game props CSV to update")
    p.add_argument("--debug", action="store_true")
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
    return " ".join((s or "").strip().lower().split())

def _loose_key(s: str) -> str:
    return _PUNCT_RE.sub("", _canon_key(s))

def build_team_mapping() -> Dict[str, str]:
    mapping = {_loose_key(k): v for k, v in TEAM_ALIASES.items()}
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
                    mapping[_loose_key(s)] = mapping.get(_loose_key(a), a)
        except Exception as e:
            print(f"⚠️ team_name_master.csv problem: {e}", file=sys.stderr)
    mapping[_loose_key("athletics")] = "oakathletics"
    return mapping

def normalize_for_match(val: str, mapping: Dict[str,str]) -> str:
    k = _loose_key(str(val or "").strip())
    return mapping.get(k, k)

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

    try:
        df_bets = pd.read_csv(args.out, keep_default_na=False)
    except FileNotFoundError:
        print(f"Error: Bet file not found at '{args.out}'.", file=sys.stderr)
        sys.exit(1)

    home_col, away_col = find_team_columns(df_bets)

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
        games_to_score.append({
            "home_team_api": normalize_for_match(home_team, mapping),
            "away_team_api": normalize_for_match(away_team, mapping),
            "home_score": game["linescore"]["teams"]["home"].get("runs"),
            "away_score": game["linescore"]["teams"]["away"].get("runs"),
        })

    for i, row in df_bets.iterrows():
        home_team_bet = normalize_for_match(row.get(home_col, ""), mapping)
        away_team_bet = normalize_for_match(row.get(away_col, ""), mapping)

        for game in games_to_score:
            if home_team_bet == game["home_team_api"] and away_team_bet == game["away_team_api"]:
                df_bets.loc[i, "home_score"] = game["home_score"]
                df_bets.loc[i, "away_score"] = game["away_score"]
                df_bets.loc[i, "actual_real_run_total"] = (game["home_score"] or 0) + (game["away_score"] or 0)
                if "total" in df_bets.columns:
                    try:
                        df_bets.loc[i, "run_total_diff"] = float(df_bets.loc[i, "actual_real_run_total"]) - float(df_bets.loc[i, "total"])
                    except ValueError:
                        df_bets.loc[i, "run_total_diff"] = None
                df_bets.loc[i, "favorite_correct"] = None  # Placeholder for your logic
                break

    df_bets.to_csv(args.out, index=False)
    print(f"Updated and saved to '{args.out}'.")

if __name__ == "__main__":
    main()
