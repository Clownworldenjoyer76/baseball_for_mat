#!/usr/bin/env python3
import argparse, csv, sys, time, unicodedata, re
from pathlib import Path
import requests
import pandas as pd

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # optional external map

# Fallback: common nicknames/aliases -> MLB StatsAPI full team names
SHORT_TO_API = {
    # AL East
    "yankees":"New York Yankees","red sox":"Boston Red Sox","blue jays":"Toronto Blue Jays",
    "rays":"Tampa Bay Rays","orioles":"Baltimore Orioles",
    # AL Central
    "guardians":"Cleveland Guardians","tigers":"Detroit Tigers","twins":"Minnesota Twins",
    "royals":"Kansas City Royals","white sox":"Chicago White Sox",
    # AL West
    "astros":"Houston Astros","mariners":"Seattle Mariners","rangers":"Texas Rangers",
    "angels":"Los Angeles Angels","athletics":"Oakland Athletics","a's":"Oakland Athletics","as":"Oakland Athletics",
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
    p = argparse.ArgumentParser(description="Score daily PLAYER props: writes prop_correct only.")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--api", default="https://statsapi.mlb.com/api/v1")
    p.add_argument("--out", required=True, help="Per-day player props CSV to update")
    p.add_argument("--dnp-as", default="DNP", choices=["DNP","No","Blank","dnp","no","blank"],
                   help="How to mark players with no boxscore stats (default: DNP)")
    return p.parse_args()

def _get(url, params=None, tries=3, sleep=0.8):
    for _ in range(tries):
        r = requests.get(url, params=params, timeout=25)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()

def norm_name_to_last_first(full_name: str) -> str:
    s = str(full_name or "").strip()
    if not s:
        return ""
    parts = s.split()
    if len(parts) == 1:
        return parts[0]
    last = parts[-1]
    first = " ".join(parts[:-1])
    return f"{last}, {first}"

def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))

_non_alnum = re.compile(r"[^a-z0-9]+")
def make_name_key(name: str) -> str:
    s = strip_accents(str(name or "").lower())
    s = _non_alnum.sub("", s)  # drop spaces, punctuation
    return s

def build_team_mapping():
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

def normalize_team_for_match(val: str, mapping) -> str:
    s = str(val or "").strip()
    low = s.lower()
    if s in mapping.values():
        return s.lower()
    return (mapping.get(low, s)).lower()

def collect_boxscore_stats_for_date(api_base: str, date: str) -> pd.DataFrame:
    """Return per-player stats for the date with strong keys (name_key + team_match)."""
    sched = _get(f"{api_base}/schedule", {"sportId":1, "date":date})
    game_pks = [g.get("gamePk") for d in sched.get("dates", []) for g in d.get("games", [])]
    rows = []
    for pk in game_pks:
        try:
            box = _get(f"{api_base}/game/{pk}/boxscore")
        except Exception:
            continue
        for side in ("home","away"):
            tjs = box.get("teams", {}).get(side)
            if not tjs:
                continue
            team_name = tjs.get("team", {}).get("name", "")
            players = []
            players.extend(tjs.get("batters", []) or [])
            players.extend(tjs.get("pitchers", []) or [])
            for pid in set(players):
                pnode = tjs.get("players", {}).get(f"ID{pid}", {})
                info = pnode.get("person", {}) or {}
                nm = norm_name_to_last_first(info.get("fullName", ""))
                stats = pnode.get("stats", {})
                bat = stats.get("batting", {}) or {}
                pit = stats.get("pitching", {}) or {}

                hits = bat.get("hits") or 0
                hr = bat.get("homeRuns") or 0
                singles = (bat.get("hits") or 0) - (bat.get("doubles") or 0) - (bat.get("triples") or 0) - (bat.get("homeRuns") or 0)
                tb = (singles or 0) + 2*(bat.get("doubles") or 0) + 3*(bat.get("triples") or 0) + 4*(bat.get("homeRuns") or 0)

                k_bat = bat.get("strikeOuts")
                k_pit = pit.get("strikeOuts")
                bb_bat = bat.get("baseOnBalls")
                bb_pit = pit.get("baseOnBalls")

                rows.append({
                    "team": team_name,
                    "player_name": nm,
                    "hits": int(hits), "home_runs": int(hr), "total_bases": int(tb),
                    "strikeouts": int(max([v for v in [k_bat, k_pit] if v is not None], default=0)),
                    "walks": int(max([v for v in [bb_bat, bb_pit] if v is not None], default=0)),
                })
    if not rows:
        return pd.DataFrame(columns=["team","player_name","hits","home_runs","total_bases","strikeouts","walks"])
    df = pd.DataFrame(rows)
    # Strong keys
    mapping = build_team_mapping()
    df["team_match"] = df["team"].apply(lambda v: normalize_team_for_match(v, mapping))
    df["name_key"] = df["player_name"].apply(make_name_key)
    # Deduplicate to max numeric per key
    df = df.groupby(["name_key","team_match"], as_index=False).max(numeric_only=True)
    return df

def map_prop_to_metric(ptype: str) -> str:
    p = str(ptype or "").strip().lower()
    return {
        "hits": "hits",
        "home_runs": "home_runs",
        "total_bases": "total_bases",
        "strikeouts": "strikeouts",
        "pitcher_strikeouts": "strikeouts",
        "walks": "walks",
        "walks_allowed": "walks",
    }.get(p, "")

def main():
    args = parse_args()
    dnp_as = args.dnp_as.upper()
    if dnp_as not in {"DNP","NO","BLANK"}:
        dnp_as = "DNP"

    out_path = Path(args.out)
    if not out_path.exists():
        print(f"❌ Per-day player props not found: {out_path}", file=sys.stderr)
        sys.exit(1)

    picks = pd.read_csv(out_path)
    for col in ["date","team","player_name","prop_type"]:
        if col not in picks.columns:
            picks[col] = ""

    # Keys for picks
    mapping = build_team_mapping()
    picks["team_match"] = picks["team"].apply(lambda v: normalize_team_for_match(v, mapping))
    picks["name_key"] = picks["player_name"].apply(lambda s: make_name_key(str(s).strip()))

    # Lines
    if "prop_line" in picks.columns:
        line_series = pd.to_numeric(picks["prop_line"], errors="coerce")
    else:
        line_series = pd.to_numeric(picks.get("line"), errors="coerce")

    # Actuals
    actual = collect_boxscore_stats_for_date(args.api, args.date)

    # Pass 1: match on (name_key, team_match)
    merged = picks.merge(actual, on=["name_key","team_match"], how="left", suffixes=("", "_actual"))

    # Pass 2: still missing → match on name_key only (max across teams)
    need = merged[["hits","home_runs","total_bases","strikeouts","walks"]].isna().all(axis=1)
    if need.any():
        actual_by_name = (actual.groupby("name_key", as_index=False)
                               .max(numeric_only=True)[["name_key","hits","home_runs","total_bases","strikeouts","walks"]])
        fill = picks.loc[need, ["name_key"]].merge(actual_by_name, on="name_key", how="left")
        merged.loc[need, ["hits","home_runs","total_bases","strikeouts","walks"]] = fill[["hits","home_runs","total_bases","strikeouts","walks"]].values

    # Decide prop_correct
    metric_for_row = [map_prop_to_metric(pt) for pt in picks["prop_type"]]
    actual_vals = []
    for i, met in enumerate(metric_for_row):
        val = merged.at[i, met] if met else None
        try:
            actual_vals.append(float(val) if pd.notna(val) else None)
        except Exception:
            actual_vals.append(None)

    def decide(av, ln, missing):
        if missing:
            if dnp_as == "DNP":   return "DNP"
            if dnp_as == "NO":    return "No"
            return ""  # BLANK
        if av is None or pd.isna(ln):
            return ""
        try:
            return "Yes" if float(av) >= float(ln) else "No"
        except Exception:
            return ""

    missing_mask = merged[["hits","home_runs","total_bases","strikeouts","walks"]].isna().all(axis=1).tolist()
    picks["prop_correct"] = [decide(av, ln, miss) for av, ln, miss in zip(actual_vals, line_series, missing_mask)]

    # Save
    picks.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Player props scored: {out_path}")

if __name__ == "__main__":
    main()
