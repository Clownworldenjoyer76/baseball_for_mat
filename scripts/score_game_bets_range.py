#!/usr/bin/env python3
import argparse, csv, sys, time
from pathlib import Path
from typing import Dict, Tuple
import requests
import pandas as pd

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # adjust if different

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
        r = requests.get(url, params=params, timeout=20)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()

def normalize_team_names(df: pd.DataFrame, home_col: str, away_col: str) -> pd.DataFrame:
    """Normalize short names in CSV to API full names using team_name_master.csv if available."""
    if TEAM_MAP_FILE.exists():
        try:
            tm = pd.read_csv(TEAM_MAP_FILE)
            tm["team_name_short"] = tm["team_name_short"].astype(str).str.strip().str.lower()
            tm["team_name_api"] = tm["team_name_api"].astype(str).str.strip()
            mapping = dict(zip(tm["team_name_short"], tm["team_name_api"]))
            df[home_col] = df[home_col].astype(str).str.strip().str.lower().replace(mapping)
            df[away_col] = df[away_col].astype(str).str.strip().str.lower().replace(mapping)
        except Exception as e:
            print(f"⚠️ Could not load/parse team map: {e}", file=sys.stderr)
    return df

def load_scores_for_date(api_base: str, date: str) -> Dict[Tuple[str,str], Tuple[int,int]]:
    """Returns {(away_team_name.lower(), home_team_name.lower()): (away_score, home_score)}."""
    js = _get(f"{api_base}/schedule", {"sportId": 1, "date": date})
    mapping = {}
    for d in js.get("dates", []):
        for g in d.get("games", []):
            linescore = g.get("linescore", {}) or {}
            away = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
            home = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
            a_runs = linescore.get("teams", {}).get("away", {}).get("runs")
            h_runs = linescore.get("teams", {}).get("home", {}).get("runs")
            if a_runs is not None and h_runs is not None:
                key = (away.strip().lower(), home.strip().lower())
                mapping[key] = (int(a_runs), int(h_runs))
    return mapping

def clean_team(s: str) -> str:
    return str(s or "").strip().lower()

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

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Normalize team names to API full names
    df = normalize_team_names(df, "home_team", "away_team")

    scores = load_scores_for_date(args.api, args.date)
    if "home_score" not in df.columns: df["home_score"] = pd.NA
    if "away_score" not in df.columns: df["away_score"] = pd.NA

    for i, r in df.iterrows():
        k = (clean_team(r.get("away_team","")), clean_team(r.get("home_team","")))
        if k in scores:
            a, h = scores[k]
            df.at[i, "away_score"] = a
            df.at[i, "home_score"] = h

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
    act = pd.to_numeric(df["actual_real_run_total"], errors="coerce")
    df["run_total_diff"] = (act - proj).where(~act.isna() & ~proj.isna(), pd.NA)

    def fav_ok(r):
        w = str(r.get("winner") or "").strip().lower()
        f = str(r.get("favorite") or "").strip().lower()
        if not w or not f:
            return ""
        return "Yes" if w == f else "No"

    df["favorite_correct"] = df.apply(fav_ok, axis=1)

    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Game bets scored: {out_path}")

if __name__ == "__main__":
    main()
