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
        r = requests.get(url, params=params, timeout=25)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()

def normalize_team_names_df(df: pd.DataFrame, home_col: str, away_col: str) -> pd.DataFrame:
    """Normalize short names in CSV to API full names using team_name_master.csv if available."""
    if TEAM_MAP_FILE.exists():
        try:
            tm = pd.read_csv(TEAM_MAP_FILE)
            tm["team_name_short"] = tm["team_name_short"].astype(str).strip().str.lower()
            tm["team_name_api"]   = tm["team_name_api"].astype(str).str.strip()
            mapping = dict(zip(tm["team_name_short"], tm["team_name_api"]))
            df[home_col] = df[home_col].astype(str).str.strip().str.lower().replace(mapping)
            df[away_col] = df[away_col].astype(str).str.strip().str.lower().replace(mapping)
        except Exception as e:
            print(f"⚠️ team map issue: {e}", file=sys.stderr)
    return df

def load_scores_for_date(api_base: str, date: str) -> Dict[Tuple[str, str], Tuple[int, int]]:
    """
    Returns {(away_full.lower(), home_full.lower()): (away_runs, home_runs)}
    Uses /schedule?hydrate=linescore to ensure runs are present when available.
    """
    js = _get(f"{api_base}/schedule", {"sportId": 1, "date": date, "hydrate": "linescore"})
    mapping: Dict[Tuple[str, str], Tuple[int, int]] = {}
    for d in js.get("dates", []):
        for g in d.get("games", []):
            away_name = (g.get("teams", {}).get("away", {}).get("team", {}) or {}).get("name", "")
            home_name = (g.get("teams", {}).get("home", {}).get("team", {}) or {}).get("name", "")
            ls = g.get("linescore", {}) or {}
            a_runs = ((ls.get("teams", {}) or {}).get("away", {}) or {}).get("runs")
            h_runs = ((ls.get("teams", {}) or {}).get("home", {}) or {}).get("runs")
            if a_runs is not None and h_runs is not None:
                mapping[(away_name.strip().lower(), home_name.strip().lower())] = (int(a_runs), int(h_runs))
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

    # Required columns present?
    required = ["date", "home_team", "away_team", "projected_real_run_total", "favorite"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Missing required columns in {out_path}: {missing}", file=sys.stderr)
        sys.exit(1)

    # Normalize date
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Normalize team names in the CSV to API full names
    df = normalize_team_names_df(df, "home_team", "away_team")

    # Ensure score columns exist
    if "home_score" not in df.columns: df["home_score"] = pd.NA
    if "away_score" not in df.columns: df["away_score"] = pd.NA

    # Fetch scores (with linescore hydrated)
    scores = load_scores_for_date(args.api, args.date)

    # Try to place scores; support both (away,home) and reversed in case CSV sides are swapped
    for i, r in df.iterrows():
        k1 = (clean_team(r.get("away_team", "")), clean_team(r.get("home_team", "")))
        k2 = (clean_team(r.get("home_team", "")), clean_team(r.get("away_team", "")))  # reversed safety
        if k1 in scores:
            a, h = scores[k1]
            df.at[i, "away_score"] = a
            df.at[i, "home_score"] = h
        elif k2 in scores:
            a, h = scores[k2]
            # If matched as reversed, swap when assigning
            df.at[i, "away_score"] = h
            df.at[i, "home_score"] = a

    # Actual total
    df["actual_real_run_total"] = (
        pd.to_numeric(df.get("home_score"), errors="coerce").fillna(pd.NA) +
        pd.to_numeric(df.get("away_score"), errors="coerce").fillna(pd.NA)
    )

    # Winner
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

    # Diff vs projected
    proj = pd.to_numeric(df["projected_real_run_total"], errors="coerce")
    act  = pd.to_numeric(df["actual_real_run_total"], errors="coerce")
    df["run_total_diff"] = (act - proj).where(~act.isna() & ~proj.isna(), pd.NA)

    # Favorite correct?
    def fav_ok(r):
        w = str(r.get("winner") or "").strip().lower()
        f = str(r.get("favorite") or "").strip().lower()
        if not w or not f:
            return ""
        return "Yes" if w == f else "No"

    df["favorite_correct"] = df.apply(fav_ok, axis=1)

    # Save
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Game bets scored: {out_path}")

if __name__ == "__main__":
    main()
