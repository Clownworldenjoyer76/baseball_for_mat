#!/usr/bin/env python3
import argparse, csv, sys, time, re
from pathlib import Path
from typing import Dict, Tuple, List, Any
import requests
import pandas as pd
import numpy as np

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # optional external map

# ---------- 30-team alias map ----------
TEAM_ALIASES: Dict[str, str] = {
    # AL EAST
    "new york yankees":"New York Yankees","yankees":"New York Yankees","nyy":"New York Yankees","ny yankees":"New York Yankees","yanks":"New York Yankees",
    "boston red sox":"Boston Red Sox","red sox":"Boston Red Sox","bos":"Boston Red Sox","bo sox":"Boston Red Sox","bo-sox":"Boston Red Sox",
    "toronto blue jays":"Toronto Blue Jays","blue jays":"Toronto Blue Jays","jays":"Toronto Blue Jays","tor":"Toronto Blue Jays",
    "tampa bay rays":"Tampa Bay Rays","rays":"Tampa Bay Rays","tb":"Tampa Bay Rays","tbr":"Tampa Bay Rays","devil rays":"Tampa Bay Rays",
    "baltimore orioles":"Baltimore Orioles","orioles": "Baltimore Orioles","o's":"Baltimore Orioles","os":"Baltimore Orioles","bal":"Baltimore Orioles",

    # AL CENTRAL
    "cleveland guardians":"Cleveland Guardians","guardians":"Cleveland Guardians","cle":"Cleveland Guardians","indians":"Cleveland Guardians",
    "detroit tigers":"Detroit Tigers","tigers":"Detroit Tigers","det":"Detroit Tigers",
    "minnesota twins":"Minnesota Twins","twins":"Minnesota Twins","min":"Minnesota Twins","twinkies":"Minnesota Twins",
    "kansas city royals":"Kansas City Royals","royals":"Kansas City Royals","kc":"Kansas City Royals","kcr":"Kansas City Royals",
    "chicago white sox":"Chicago White Sox","white sox":"Chicago White Sox","whitesox":"Chicago White Sox","chisox":"Chicago White Sox","cws":"Chicago White Sox","chw":"Chicago White Sox",

    # AL WEST
    "houston astros":"Houston Astros","astros":"Houston Astros","hou":"Houston Astros","stros":"Houston Astros",
    "seattle mariners":"Seattle Mariners","mariners":"Seattle Mariners","sea":"Seattle Mariners","m's":"Seattle Mariners","ms":"Seattle Mariners",
    "texas rangers":"Texas Rangers","rangers":"Texas Rangers","tex":"Texas Rangers",
    "los angeles angels":"Los Angeles Angels","angels":"Los Angeles Angels","laa":"Los Angeles Angels","ana":"Los Angeles Angels","halos":"Los Angeles Angels",

    # Athletics mappings
    "oakland athletics": "Athletics","athletics": "Athletics","oak":"Athletics","a's":"Athletics","as":"Athletics","a s":"Athletics",
    
    # NL EAST
    "atlanta braves":"Atlanta Braves","braves":"Atlanta Braves","atl":"Atlanta Braves",
    "miami marlins":"Miami Marlins","marlins":"Miami Marlins","mia":"Miami Marlins","fish":"Miami Marlins",
    "new york mets":"New York Mets","mets":"New York Mets","nym":"New York Mets","metropolitans":"New York Mets",
    "philadelphia phillies":"Philadelphia Phillies","phillies":"Philadelphia Phillies","phils":"Philadelphia Phillies","phi":"Philadelphia Phillies",
    "washington nationals":"Washington Nationals","nationals":"Washington Nationals","nats":"Washington Nationals","was":"Washington Nationals","wsh":"Washington Nationals",

    # NL CENTRAL
    "chicago cubs":"Chicago Cubs","cubs":"Chicago Cubs","chc":"Chicago Cubs","cubbies":"Chicago Cubs",
    "st. louis cardinals":"St. Louis Cardinals","st louis cardinals":"St. Louis Cardinals","cardinals":"St. Louis Cardinals","stl":"St. Louis Cardinals","redbirds":"St. Louis Cardinals",
    "milwaukee brewers":"Milwaukee Brewers","brewers":"Milwaukee Brewers","mil":"Milwaukee Brewers","brew crew":"Milwaukee Brewers",
    "cincinnati reds":"Cincinnati Reds","reds":"Cincinnati Reds","cin":"Cincinnati Reds","big red machine":"Cincinnati Reds",
    "pittsburgh pirates":"Pittsburgh Pirates","pirates":"Pittsburgh Pirates","pit":"Pittsburgh Pirates","bucs":"Pittsburgh Pirates","buccos":"Pittsburgh Pirates",

    # NL WEST
    "los angeles dodgers":"Los Angeles Dodgers","la dodgers":"Los Angeles Dodgers","dodgers":"Los Angeles Dodgers","lad":"Los Angeles Dodgers",
    "san francisco giants":"San Francisco Giants","giants":"San Francisco Giants","sf":"San Francisco Giants","sfg":"San Francisco Giants",
    "san diego padres":"San Diego Padres","padres":"San Diego Padres","sd":"San Diego Padres","friars":"San Diego Padres",
    "arizona diamondbacks":"Arizona Diamondbacks","diamondbacks":"Arizona Diamondbacks","dbacks":"Arizona Diamondbacks","d-backs":"Arizona Diamondbacks","ari":"Arizona Diamondbacks","snakes":"Arizona Diamondbacks",
    "colorado rockies":"Colorado Rockies","rockies":"Colorado Rockies","col":"Colorado Rockies","rox":"Colorado Rockies",
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

def _sanitize_key(s: str) -> str:
    """A more aggressive key for matching that removes all non-alphanumeric characters."""
    s = str(s or "").strip().lower()
    return re.sub(r'[^a-zA-Z0-9]', '', s)

def build_team_mapping() -> Dict[str, str]:
    # Use the more aggressive sanitization for mapping keys
    mapping = { _sanitize_key(k): v for k, v in TEAM_ALIASES.items() }

    # Add identity mappings for all 30 full names
    fulls = [
        "New York Yankees","Boston Red Sox","Toronto Blue Jays","Tampa Bay Rays","Baltimore Orioles",
        "Cleveland Guardians","Detroit Tigers","Minnesota Twins","Kansas City Royals","Chicago White Sox",
        "Houston Astros","Seattle Mariners","Texas Rangers","Los Angeles Angels","Athletics",
        "Atlanta Braves","Miami Marlins","New York Mets","Philadelphia Phillies","Washington Nationals",
        "Chicago Cubs","St. Louis Cardinals","Milwaukee Brewers","Cincinnati Reds","Pittsburgh Pirates",
        "Los Angeles Dodgers","San Francisco Giants","San Diego Padres","Arizona Diamondbacks","Colorado Rockies",
    ]
    for f in fulls:
        mapping[_sanitize_key(f)] = f

    # Extend from CSV if present
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
                    mapping[_sanitize_key(s)] = a
        except Exception as e:
            print(f"⚠️ team_name_master.csv problem: {e}", file=sys.stderr)
    return mapping

def normalize_for_match(val: str, mapping: Dict[str,str]) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    
    k = _sanitize_key(s)
    if k in mapping:
        # Return the normalized value, which is already a string of alphanumeric chars
        return _sanitize_key(mapping[k])
    
    # If not in the mapping, return the sanitized version of the original string
    return k

def main():
    args = parse_args()
    mapping = build_team_mapping()

    # Read the existing CSV file into a DataFrame
    try:
        df_bets = pd.read_csv(args.out, keep_default_na=False)
    except FileNotFoundError:
        print(f"Error: Bet file not found at '{args.out}'.", file=sys.stderr)
        sys.exit(1)
    
    # Ensure necessary columns exist to avoid KeyErrors
    for col in ["home_score", "away_score", "game_found"]:
        if col not in df_bets.columns:
            df_bets[col] = pd.NA
        
    # Get the game schedule from MLB API
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
                "home_score": game["linescore"]["teams"]["home"]["runs"],
                "away_score": game["linescore"]["teams"]["away"]["runs"],
            })

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from MLB API: {e}", file=sys.stderr)
        sys.exit(1)

    matched_bets_indices = []
    print(f"Scoring {len(df_bets)} bets from '{args.out}' against {len(games_to_score)} games for {args.date}...")

    # A more robust matching loop
    for i, row in df_bets.iterrows():
        best_match = None
        best_match_score = 0
        
        home_team_bet = normalize_for_match(row.get("HOME", ""), mapping)
        away_team_bet = normalize_for_match(row.get("AWAY", ""), mapping)
        
        if args.debug:
            print(f"DEBUG: Processing bet for: '{home_team_bet}' vs '{away_team_bet}'")

        for game in games_to_score:
            game_home = game['home_team_api']
            game_away = game['away_team_api']
            current_score = 0
            
            if args.debug:
                print(f"DEBUG: Comparing against game: '{game_home}' vs '{game_away}'")

            # Score for a perfect match (both teams)
            if (home_team_bet == game_home and away_team_bet == game_away) or \
               (home_team_bet == game_away and away_team_bet == game_home):
                current_score = 2
            # Score for a one-team match
            elif (home_team_bet == game_home or home_team_bet == game_away) or \
                 (away_team_bet == game_home or away_team_bet == game_away):
                current_score = 1
                
            if current_score > best_match_score:
                best_match_score = current_score
                best_match = game

        # If a match was found, process it
        if best_match and i not in matched_bets_indices:
            print(f"✅ Found match (score {best_match_score}) for gamePk {best_match['gamePk']}: Bet on {row['AWAY']} vs {row['HOME']}.")
            
            if pd.isna(df_bets.loc[i, "home_score"]):
                df_bets.loc[i, "home_score"] = best_match["home_score"]
            if pd.isna(df_bets.loc[i, "away_score"]):
                df_bets.loc[i, "away_score"] = best_match["away_score"]
            if pd.isna(df_bets.loc[i, "game_found"]):
                df_bets.loc[i, "game_found"] = True
            
            matched_bets_indices.append(i)
    
    # After the loop, mark all unmatched bets as False
    unmatched_indices = [i for i in df_bets.index if i not in matched_bets_indices]
    for i in unmatched_indices:
         if pd.isna(df_bets.loc[i, "game_found"]):
            df_bets.loc[i, "game_found"] = False

    # Save the updated DataFrame, overwriting the original file
    df_bets.to_csv(args.out, index=False)
    
    print("\n--- Process complete ---")
    print(f"Updated {len(matched_bets_indices)} bets and saved to '{args.out}'.")

if __name__ == "__main__":
    main()
