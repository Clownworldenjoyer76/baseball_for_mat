#!/usr/bin/env python3
import argparse, csv, sys, time, re
from pathlib import Path
from typing import Dict, Tuple
import requests
import pandas as pd

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # optional external map

# Full 30-team alias map (lowercased keys) -> MLB StatsAPI full team names
TEAM_ALIASES: Dict[str, str] = {
    # ===================== AL EAST =====================
    # Yankees
    "yankees":"New York Yankees","nyy":"New York Yankees","new york yankees":"New York Yankees",
    "ny yankees":"New York Yankees","bronx bombers":"New York Yankees","yanks":"New York Yankees",
    # Red Sox
    "red sox":"Boston Red Sox","bos":"Boston Red Sox","boston red sox":"Boston Red Sox",
    "boston":"Boston Red Sox","sox boston":"Boston Red Sox","bo sox":"Boston Red Sox","bo-sox":"Boston Red Sox",
    # Blue Jays
    "blue jays":"Toronto Blue Jays","jays":"Toronto Blue Jays","tor":"Toronto Blue Jays",
    "toronto":"Toronto Blue Jays","toronto blue jays":"Toronto Blue Jays",
    # Rays
    "rays":"Tampa Bay Rays","tb":"Tampa Bay Rays","tbr":"Tampa Bay Rays","tampa bay":"Tampa Bay Rays",
    "tampa bay rays":"Tampa Bay Rays","tampa":"Tampa Bay Rays","devil rays":"Tampa Bay Rays",
    # Orioles
    "orioles":"Baltimore Orioles","o's":"Baltimore Orioles","os":"Baltimore Orioles","bal":"Baltimore Orioles",
    "baltimore":"Baltimore Orioles","baltimore orioles":"Baltimore Orioles",

    # ===================== AL CENTRAL =====================
    # Guardians
    "guardians":"Cleveland Guardians","cle":"Cleveland Guardians","cleveland":"Cleveland Guardians",
    "cleveland guardians":"Cleveland Guardians","indians":"Cleveland Guardians","cleveland indians":"Cleveland Guardians",
    # Tigers
    "tigers":"Detroit Tigers","det":"Detroit Tigers","detroit":"Detroit Tigers",
    "detroit tigers":"Detroit Tigers","motor city kitties":"Detroit Tigers",
    # Twins
    "twins":"Minnesota Twins","min":"Minnesota Twins","minn":"Minnesota Twins",
    "minnesota":"Minnesota Twins","minnesota twins":"Minnesota Twins","twinkies":"Minnesota Twins",
    # Royals
    "royals":"Kansas City Royals","kc":"Kansas City Royals","kcr":"Kansas City Royals",
    "kansas city":"Kansas City Royals","kansas city royals":"Kansas City Royals",
    # White Sox
    "white sox":"Chicago White Sox","whitesox":"Chicago White Sox","chi sox":"Chicago White Sox","chisox":"Chicago White Sox",
    "cws":"Chicago White Sox","chw":"Chicago White Sox","chicago white sox":"Chicago White Sox",
    "south siders":"Chicago White Sox","white socks":"Chicago White Sox",

    # ===================== AL WEST =====================
    # Astros
    "astros":"Houston Astros","hou":"Houston Astros","houston":"Houston Astros",
    "houston astros":"Houston Astros","stros":"Houston Astros","’stros":"Houston Astros","stros!":"Houston Astros",
    # Mariners
    "mariners":"Seattle Mariners","sea":"Seattle Mariners","seattle":"Seattle Mariners",
    "seattle mariners":"Seattle Mariners","m's":"Seattle Mariners","ms":"Seattle Mariners","m-s":"Seattle Mariners",
    # Rangers
    "rangers":"Texas Rangers","tex":"Texas Rangers","txr":"Texas Rangers",
    "texas":"Texas Rangers","texas rangers":"Texas Rangers",
    # Angels
    "angels":"Los Angeles Angels","laa":"Los Angeles Angels","ana":"Los Angeles Angels",
    "los angeles angels":"Los Angeles Angels","los angeles angels of anaheim":"Los Angeles Angels",
    "anaheim angels":"Los Angeles Angels","anaheim":"Los Angeles Angels","halos":"Los Angeles Angels",
    # Athletics
    "athletics":"Oakland Athletics","oakland athletics":"Oakland Athletics","oak":"Oakland Athletics",
    "a's":"Oakland Athletics","as":"Oakland Athletics","a s":"Oakland Athletics","oakland a's":"Oakland Athletics",
    "oakland as":"Oakland Athletics","oakland":"Oakland Athletics","elephant herd":"Oakland Athletics",

    # ===================== NL EAST =====================
    # Braves
    "braves":"Atlanta Braves","atl":"Atlanta Braves","atlanta":"Atlanta Braves","atlanta braves":"Atlanta Braves",
    # Marlins
    "marlins":"Miami Marlins","mia":"Miami Marlins","miami":"Miami Marlins","miami marlins":"Miami Marlins",
    "fish":"Miami Marlins","fishes":"Miami Marlins",
    # Mets
    "mets":"New York Mets","nym":"New York Mets","new york mets":"New York Mets",
    "ny mets":"New York Mets","amazins":"New York Mets","metropolitans":"New York Mets",
    # Phillies
    "phillies":"Philadelphia Phillies","phi":"Philadelphia Phillies","phils":"Philadelphia Phillies",
    "philadelphia":"Philadelphia Phillies","philadelphia phillies":"Philadelphia Phillies",
    # Nationals
    "nationals":"Washington Nationals","nats":"Washington Nationals","was":"Washington Nationals","wsh":"Washington Nationals",
    "washington":"Washington Nationals","washington nationals":"Washington Nationals",

    # ===================== NL CENTRAL =====================
    # Cubs
    "cubs":"Chicago Cubs","chc":"Chicago Cubs","chicago cubs":"Chicago Cubs","cubbies":"Chicago Cubs",
    # Cardinals
    "cardinals":"St. Louis Cardinals","stl":"St. Louis Cardinals","st. louis":"St. Louis Cardinals",
    "saint louis":"St. Louis Cardinals","st louis":"St. Louis Cardinals","st. louis cardinals":"St. Louis Cardinals",
    "st louis cardinals":"St. Louis Cardinals","saint louis cardinals":"St. Louis Cardinals","redbirds":"St. Louis Cardinals",
    # Brewers
    "brewers":"Milwaukee Brewers","mil":"Milwaukee Brewers","milwaukee":"Milwaukee Brewers",
    "milwaukee brewers":"Milwaukee Brewers","brew crew":"Milwaukee Brewers","brew-crew":"Milwaukee Brewers",
    # Reds
    "reds":"Cincinnati Reds","cin":"Cincinnati Reds","cincinnati":"Cincinnati Reds","cincinnati reds":"Cincinnati Reds",
    "big red machine":"Cincinnati Reds",
    # Pirates
    "pirates":"Pittsburgh Pirates","pit":"Pittsburgh Pirates","pittsburgh":"Pittsburgh Pirates",
    "pittsburgh pirates":"Pittsburgh Pirates","bucs":"Pittsburgh Pirates","buccos":"Pittsburgh Pirates",

    # ===================== NL WEST =====================
    # Dodgers
    "dodgers":"Los Angeles Dodgers","lad":"Los Angeles Dodgers","la dodgers":"Los Angeles Dodgers",
    "los angeles dodgers":"Los Angeles Dodgers","dodgers la":"Los Angeles Dodgers","boys in blue":"Los Angeles Dodgers",
    # Giants
    "giants":"San Francisco Giants","sf":"San Francisco Giants","sfg":"San Francisco Giants",
    "san francisco":"San Francisco Giants","san francisco giants":"San Francisco Giants","orange and black":"San Francisco Giants",
    # Padres
    "padres":"San Diego Padres","sd":"San Diego Padres","sdg":"San Diego Padres","san diego":"San Diego Padres",
    "san diego padres":"San Diego Padres","friars":"San Diego Padres",
    # Diamondbacks
    "diamondbacks":"Arizona Diamondbacks","dbacks":"Arizona Diamondbacks","d-backs":"Arizona Diamondbacks",
    "dbacks":"Arizona Diamondbacks","d backs":"Arizona Diamondbacks","ari":"Arizona Diamondbacks",
    "arizona":"Arizona Diamondbacks","arizona diamondbacks":"Arizona Diamondbacks","snakes":"Arizona Diamondbacks",
    # Rockies
    "rockies":"Colorado Rockies","col":"Colorado Rockies","colorado":"Colorado Rockies",
    "colorado rockies":"Colorado Rockies","rox":"Colorado Rockies",
}

def parse_args():
    p = argparse.ArgumentParser(
        description="Score daily GAME bets: fills scores and writes actual_real_run_total, run_total_diff, favorite_correct."
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

def build_team_mapping() -> Dict[str, str]:
    mapping = TEAM_ALIASES.copy()

    # Also include exact full names as identity mappings (lowercased key)
    fulls = {
        "New York Yankees","Boston Red Sox","Toronto Blue Jays","Tampa Bay Rays","Baltimore Orioles",
        "Cleveland Guardians","Detroit Tigers","Minnesota Twins","Kansas City Royals","Chicago White Sox",
        "Houston Astros","Seattle Mariners","Texas Rangers","Los Angeles Angels","Oakland Athletics",
        "Atlanta Braves","Miami Marlins","New York Mets","Philadelphia Phillies","Washington Nationals",
        "Chicago Cubs","St. Louis Cardinals","Milwaukee Brewers","Cincinnati Reds","Pittsburgh Pirates",
        "Los Angeles Dodgers","San Francisco Giants","San Diego Padres","Arizona Diamondbacks","Colorado Rockies",
    }
    for f in fulls:
        mapping[f.lower()] = f

    # Optionally extend from CSV
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

# Basic cleanup: lowercase, collapse spaces, strip punctuation that commonly varies
_PUNCT_RE = re.compile(r"[.\u2019'’`-]")  # periods & common apostrophes/hyphens

def _canon_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = " ".join(s.split())  # collapse inner whitespace
    return s

def _loose_key(s: str) -> str:
    # remove periods/apostrophes/hyphens to catch "st louis", "a's", "d-backs", "m's"
    return _PUNCT_RE.sub("", _canon_key(s))

def normalize_for_match(val: str, mapping: Dict[str,str]) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    # if the value is already an exact full name in mapping values, trust it
    if s in mapping.values():
        return s.lower()

    k1 = _canon_key(s)
    k2 = _loose_key(s)

    if k1 in mapping:
        return mapping[k1].lower()
    if k2 in mapping:
        return mapping[k2].lower()

    # Try "city nickname" pattern if provided as two tokens in reverse/casual forms
    # (e.g., "boston sox" -> Boston Red Sox, "la dodgers" -> Los Angeles Dodgers)
    tokens = k1.split()
    if len(tokens) >= 2:
        # rebuild without common connectors
        guess = " ".join(tokens[-2:])  # last two words
        if guess in mapping:
            return mapping[guess].lower()
        guess2 = _loose_key(guess)
        if guess2 in mapping:
            return mapping[guess2].lower()

    # Fallback: return original lowercased
    return k1

def load_scores_for_date(api_base: str, date: str) -> Dict[Tuple[str, str], Tuple[int, int]]:
    """Returns {(away_full.lower(), home_full.lower()): (away_runs, home_runs)} using schedule?hydrate=linescore."""
    js = _get(f"{api_base}/schedule", {"sportId": 1, "date": date, "hydrate": "linescore"})
    out: Dict[Tuple[str,str], Tuple[int,int]] = {}
    for d in js.get("dates", []):
        for g in d.get("games", []):
            away_name = (g.get("teams", {}).get("away", {}).get("team", {}) or {}).get("name", "")
            home_name = (g.get("teams", {}).get("home", {}).get("team", {}) or {}).get("name", "")
            ls = g.get("linescore", {}) or {}
            a_runs = ((ls.get("teams", {}) or {}).get("away", {}) or {}).get("runs")
            h_runs = ((ls.get("teams", {}) or {}).get("home", {}) or {}).get("runs")
            if a_runs is not None and h_runs is not None:
                out[(away_name.strip().lower(), home_name.strip().lower())] = (int(a_runs), int(h_runs))
    return out

def hydrate_missing_from_linescore(api_base: str, date: str, scores: Dict[Tuple[str,str], Tuple[int,int]]):
    """If schedule lacked a linescore for some games, fetch /game/{pk}/linescore and fill."""
    sched = _get(f"{api_base}/schedule", {"sportId":1, "date":date})
    for d in sched.get("dates", []):
        for g in d.get("games", []):
            away = (g.get("teams", {}).get("away", {}).get("team", {}) or {}).get("name","").strip().lower()
            home = (g.get("teams", {}).get("home", {}).get("team", {}) or {}).get("name","").strip().lower()
            key = (away, home)
            if key in scores:
                continue
            pk = g.get("gamePk")
            if not pk:
                continue
            try:
                ls = _get(f"{api_base}/game/{pk}/linescore")
                a_runs = ((ls.get("teams", {}) or {}).get("away", {}) or {}).get("runs")
                h_runs = ((ls.get("teams", {}) or {}).get("home", {}) or {}).get("runs")
                if a_runs is not None and h_runs is not None:
                    scores[key] = (int(a_runs), int(h_runs))
            except Exception:
                pass

def main():
    args = parse_args()
    out_path = Path(args.out)
    if not out_path.exists():
        print(f"❌ File not found: {out_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(out_path)
    required = ["date", "home_team", "away_team", "projected_real_run_total", "favorite"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Missing required columns in {out_path}: {missing}", file=sys.stderr)
        sys.exit(1)

    # Keep original casing to write back unchanged
    orig_home = df["home_team"].copy()
    orig_away = df["away_team"].copy()

    # Normalize date
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Build mapping and prepare match keys (lowercased full names)
    mapping = build_team_mapping()
    df["_home_match"] = df["home_team"].apply(lambda v: normalize_for_match(v, mapping))
    df["_away_match"] = df["away_team"].apply(lambda v: normalize_for_match(v, mapping))

    # Ensure score columns exist
    if "home_score" not in df.columns: df["home_score"] = pd.NA
    if "away_score" not in df.columns: df["away_score"] = pd.NA

    # Fetch scores + per-game fallback
    scores = load_scores_for_date(args.api, args.date)
    hydrate_missing_from_linescore(args.api, args.date, scores)

    # Fill scores (try both orientations just in case)
    for i, r in df.iterrows():
        k1 = (r["_away_match"], r["_home_match"])
        k2 = (r["_home_match"], r["_away_match"])  # reversed
        if k1 in scores:
            a, h = scores[k1]
            df.at[i, "away_score"] = a
            df.at[i, "home_score"] = h
        elif k2 in scores:
            a, h = scores[k2]
            df.at[i, "away_score"] = h
            df.at[i, "home_score"] = a

    # Derived columns
    df["actual_real_run_total"] = (
        pd.to_numeric(df.get("home_score"), errors="coerce").fillna(pd.NA) +
        pd.to_numeric(df.get("away_score"), errors="coerce").fillna(pd.NA)
    )

    def winner_row(r):
        try:
            hs = float(r["home_score"])
            as_ = float(r["away_score"])
        except Exception:
            return ""
        if pd.isna(hs) or pd.isna(as_):
            return ""
        return r["home_team"] if hs > as_ else r["away_team"]
    df["winner"] = df.apply(winner_row, axis=1)

    proj = pd.to_numeric(df["projected_real_run_total"], errors="coerce")
    act  = pd.to_numeric(df["actual_real_run_total"], errors="coerce")
    df["run_total_diff"] = (act - proj).where(~act.isna() & ~proj.isna(), pd.NA)

    def fav_ok(r):
        w = str(r.get("winner") or "").strip().lower()
        f = str(r.get("favorite") or "").strip().lower()
        if not w or not f:
            return ""
        return "Yes" if w == f else "No"
    df["favorite_correct"] = df.apply(fav_ok, axis=1)

    # Restore original casing for team columns
    df["home_team"] = orig_home
    df["away_team"] = orig_away

    # Drop helper cols
    df.drop(columns=["_home_match","_away_match"], inplace=True, errors="ignore")

    # Save
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Game bets scored: {out_path}")

if __name__ == "__main__":
    main()
