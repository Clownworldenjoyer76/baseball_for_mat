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

def _canon_key(s: str) -> str:
    s = (s or "").strip().lower()
    return " ".join(s.split())

def _loose_key(s: str) -> str:
    return _PUNCT_RE.sub("", _canon_key(s))

def normalize_for_match(val: str, mapping: Dict[str,str]) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    if s in mapping.values():
        return s.lower()
    k1 = _canon_key(s)
    k2 = _loose_key(s)
    if k1 in mapping: return mapping[k1].lower()
    if k2 in mapping: return mapping[k2].lower()
    # last-two-token heuristic
    toks = k1.split()
    if len(toks) >= 2:
        guess = " ".join(toks[-2:])
        if guess in mapping: return mapping[guess].lower()
        g2 = _loose_key(guess)
        if g2 in mapping: return mapping[g2].lower()
    return k1

def load_scores_for_date(api_base: str, date: str) -> Dict[Tuple[str, str], Tuple[int, int]]:
    js = _get(f"{api_base}/schedule", {"sportId": 1, "date": date, "hydrate": "linescore"})
    out: Dict[Tuple[str,str], Tuple[int,int]] = {}
    for d in js.get("dates", []):
        for g in d.get("games", []):
            away = (g.get("teams", {}).get("away", {}).get("team", {}) or {}).get("name", "")
            home = (g.get("teams", {}).get("home", {}).get("team", {}) or {}).get("name", "")
            ls = g.get("linescore", {}) or {}
            a_runs = ((ls.get("teams", {}) or {}).get("away", {}) or {}).get("runs")
            h_runs = ((ls.get("teams", {}) or {}).get("home", {}) or {}).get("runs")
            if a_runs is not None and h_runs is not None:
                out[(away.strip().lower(), home.strip().lower())] = (int(a_runs), int(h_runs))
    return out

def hydrate_missing_from_linescore(api_base: str, date: str, scores: Dict[Tuple[str,str], Tuple[int,int]]):
    sched = _get(f"{api_base}/schedule", {"sportId":1, "date":date})
    for d in sched.get("dates", []):
        for g in d.get("games", []):
            away = (g.get("teams", {}).get("away", {}).get("team", {}) or {}).get("name","").strip().lower()
            home = (g.get("teams", {}).get("home", {}).get("team", {}) or {}).get("name","").strip().lower()
            if (away, home) in scores: continue
            pk = g.get("gamePk")
            if not pk: continue
            try:
                ls = _get(f"{api_base}/game/{pk}/linescore")
                a_runs = ((ls.get("teams", {}) or {}).get("away", {}) or {}).get("runs")
                h_runs = ((ls.get("teams", {}) or {}).get("home", {}) or {}).get("runs")
                if a_runs is not None and h_runs is not None:
                    scores[(away, home)] = (int(a_runs), int(h_runs))
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

    # Only operate on requested date rows
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    mask = (df["date"] == args.date)
    if not mask.any():
        print(f"⚠️ No rows for date {args.date} in {out_path}", file=sys.stderr)

    # Preserve original casing
    orig_home = df.loc[mask, "home_team"].copy()
    orig_away = df.loc[mask, "away_team"].copy()

    # Build mapping & keys
    mapping = build_team_mapping()
    df.loc[mask, "_home_match"] = df.loc[mask, "home_team"].apply(lambda v: normalize_for_match(v, mapping))
    df.loc[mask, "_away_match"] = df.loc[mask, "away_team"].apply(lambda v: normalize_for_match(v, mapping))

    # Ensure score cols
    if "home_score" not in df.columns: df["home_score"] = pd.NA
    if "away_score" not in df.columns: df["away_score"] = pd.NA

    scores = load_scores_for_date(args.api, args.date)
    hydrate_missing_from_linescore(args.api, args.date, scores)

    unmatched: List[dict] = []
    updated = 0

    for i, r in df.loc[mask].iterrows():
        k1 = (str(r["_away_match"]), str(r["_home_match"]))
        k2 = (str(r["_home_match"]), str(r["_away_match"]))  # reversed
        if k1 in scores:
            a, h = scores[k1]
            df.at[i, "away_score"] = a
            df.at[i, "home_score"] = h
            updated += 1
        elif k2 in scores:
            a, h = scores[k2]
            df.at[i, "away_score"] = h
            df.at[i, "home_score"] = a
            updated += 1
        else:
            unmatched.append({
                "row_index": i,
                "away_team": r["away_team"],
                "home_team": r["home_team"],
                "away_norm": r["_away_match"],
                "home_norm": r["_home_match"],
            })

    # Derived
    df.loc[mask, "actual_real_run_total"] = (
        pd.to_numeric(df.loc[mask, "home_score"], errors="coerce") +
        pd.to_numeric(df.loc[mask, "away_score"], errors="coerce")
    )

    def winner_row(r):
        try:
            hs = float(r["home_score"]); as_ = float(r["away_score"])
        except Exception:
            return ""
        if pd.isna(hs) or pd.isna(as_): return ""
        return r["home_team"] if hs > as_ else r["away_team"]

    df.loc[mask, "winner"] = df.loc[mask].apply(winner_row, axis=1)

    proj = pd.to_numeric(df.loc[mask, "projected_real_run_total"], errors="coerce")
    act  = pd.to_numeric(df.loc[mask, "actual_real_run_total"], errors="coerce")
    df.loc[mask, "run_total_diff"] = (act - proj).where(~act.isna() & ~proj.isna(), pd.NA)

    def fav_ok(r):
        w = str(r.get("winner") or "").strip().lower()
        f = str(r.get("favorite") or "").strip().lower()
        if not w or not f: return ""
        return "Yes" if w == f else "No"

    df.loc[mask, "favorite_correct"] = df.loc[mask].apply(fav_ok, axis=1)

    # Restore original casing for the affected rows
    df.loc[mask, "home_team"] = orig_home
    df.loc[mask, "away_team"] = orig_away

    # Cleanup
    df.drop(columns=["_home_match","_away_match"], inplace=True, errors="ignore")
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # Debug output
    if args.debug:
        print(f"🧪 date: {args.date}")
        print(f"🔢 games in CSV (date match): {int(mask.sum())}")
        print(f"📊 scores fetched: {len(scores)}")
        print(f"✅ rows updated: {updated}")
        if unmatched:
            rep = out_path.with_name(f"unmatched_games_{args.date}.csv")
            pd.DataFrame(unmatched).to_csv(rep, index=False)
            print(f"❓ unmatched rows: {len(unmatched)} -> {rep}")

    # Final status (quiet)
    if not args.debug:
        print(f"✅ Game bets scored: {out_path} (updated {updated} rows)")

if __name__ == "__main__":
    main()
