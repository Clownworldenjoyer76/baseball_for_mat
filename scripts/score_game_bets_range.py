#!/usr/bin/env python3
import argparse, csv, sys, time
from pathlib import Path
from typing import Dict, Tuple
import requests
import pandas as pd

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # optional external map

# Fallback map: common nicknames/aliases -> MLB StatsAPI full team names
SHORT_TO_API = {
    # AL East
    "yankees":"New York Yankees","red sox":"Boston Red Sox","blue jays":"Toronto Blue Jays",
    "rays":"Tampa Bay Rays","orioles":"Baltimore Orioles",
    # AL Central
    "guardians":"Cleveland Guardians","tigers":"Detroit Tigers","twins":"Minnesota Twins",
    "royals":"Kansas City Royals","white sox":"Chicago White Sox",
    # AL West
    "astros":"Houston Astros","mariners":"Seattle Mariners","rangers":"Texas Rangers",
    "angels":"Los Angeles Angels",
    "athletics":"Oakland Athletics","a's":"Oakland Athletics","as":"Oakland Athletics",
    # NL East
    "braves":"Atlanta Braves","marlins":"Miami Marlins","mets":"New York Mets",
    "phillies":"Philadelphia Phillies","nationals":"Washington Nationals",
    # NL Central
    "cubs":"Chicago Cubs","cardinals":"St. Louis Cardinals","brewers":"Milwaukee Brewers",
    "reds":"Cincinnati Reds","pirates":"Pittsburgh Pirates",
    # NL West
    "dodgers":"Los Angeles Dodgers","giants":"San Francisco Giants","padres":"San Diego Padres",
    "diamondbacks":"Arizona Diamondbacks","dbacks":"Arizona Diamondbacks","d-backs":"Arizona Diamondbacks",
    "rockies":"Colorado Rockies",
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
    mapping = SHORT_TO_API.copy()
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

def normalize_for_match(val: str, mapping: Dict[str,str]) -> str:
    s = str(val or "").strip()
    low = s.lower()
    if s in mapping.values():
        return s.lower()
    return (mapping.get(low, s)).lower()

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
