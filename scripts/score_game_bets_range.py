#!/usr/bin/env python3
import argparse, csv, sys, time, re
from pathlib import Path
from typing import Dict, Tuple, List
import requests
import pandas as pd

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # optional external map

# ---------- 30-team alias map ----------
TEAM_ALIASES: Dict[str, str] = {
    # AL EAST
    "new york yankees":"New York Yankees","yankees":"New York Yankees","nyy":"New York Yankees","ny yankees":"New York Yankees","yanks":"New York Yankees",
    "boston red sox":"Boston Red Sox","red sox":"Boston Red Sox","bos":"Boston Red Sox","bo sox":"Boston Red Sox","bo-sox":"Boston Red Sox",
    "toronto blue jays":"Toronto Blue Jays","blue jays":"Toronto Blue Jays","jays":"Toronto Blue Jays","tor":"Toronto Blue Jays",
    "tampa bay rays":"Tampa Bay Rays","rays":"Tampa Bay Rays","tb":"Tampa Bay Rays","tbr":"Tampa Bay Rays","devil rays":"Tampa Bay Rays",
    "baltimore orioles":"Baltimore Orioles","orioles":"Baltimore Orioles","o's":"Baltimore Orioles","os":"Baltimore Orioles","bal":"Baltimore Orioles",

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

    # Use MLB API naming: "Athletics"
    "oakland athletics":"Athletics","athletics":"Athletics","oak":"Athletics","a's":"Athletics","as":"Athletics","a s":"Athletics",
    
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

_PUNCT_RE = re.compile(r"[.\u2019'’`-]")  # ., apostrophes, hyphens

def parse_args():
    p = argparse.ArgumentParser(description="Score daily GAME bets and compute run_total_diff & favorite_correct.")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--api", default="https://statsapi.mlb.com/api/v1")
    p.add_argument("--out", required=True, help="Per-day game props CSV to update")
    p.add_argument("--debug", action="store_true", help="Print match diagnostics and write unmatched report")
    return p.parse_args()

def _get(url, params=None, tries=3, sleep=0.8):
    for _ in range(tries):
        r = requests.get(url, params=params, timeout=25)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()

def build_team_mapping() -> Dict[str, str]:
    mapping = TEAM_ALIASES.copy()

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
        mapping[f.lower()] = f

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
                mapping.update(dict(zip(tm["_short"], tm["_api"])))
        except Exception as e:
            print(f"⚠️ team_name_master.csv problem: {e}", file=sys.stderr)
    return mapping

# Rest of the code remains unchanged...
