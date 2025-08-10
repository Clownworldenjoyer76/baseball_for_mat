#!/usr/bin/env python3
import argparse, csv, sys, time
from pathlib import Path
from typing import Dict, Tuple
import requests
import pandas as pd

def parse_args():
    p = argparse.ArgumentParser(
        description="Score daily GAME bets: fill scores if missing, then write actual_real_run_total, run_total_diff, favorite_correct into the per-day CSV."
    )
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--api", default="https://statsapi.mlb.com/api/v1")
    p.add_argument("--out", required=True, help="Per-day game props CSV to update")
    return p.parse_args()

def _get(url, params=None, tries=3, sleep=0.8):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=20)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()

def load_scores_for_date(api_base: str, date: str) -> Dict[Tuple[str,str], Tuple[int,int]]:
    js = _get(f"{api_base}/schedule", {"sportId":1, "date": date})
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

    need_fetch = False
    for col in ("home_score","away_score"):
        if col not in df.columns or df[col].isna().all():
            need_fetch = True
    if need_fetch:
        scores = load_scores_for_date(args.api, args.date)
        if "home_score" not in df.columns: df["home_score"] = pd.NA
        if "away_score" not in df.columns: df["away_score"] = pd.NA

        for i, r in df.iterrows():
            k1 = (clean_team(r.get("away_team","")), clean_team(r.get("home_team","")))
            k2 = (clean_team(r.get("home_team","")), clean_team(r.get("away_team","")))
            if k1 in scores:
                a,h = scores[k1]
                df.at[i, "away_score"] = a
                df.at[i, "home_score"] = h
            elif k2 in scores:
                a,h = scores[k2]
                df.at[i, "away_score"] = h
                df.at[i, "home_score"] = a

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
        if pd.isna(hs) or pd.isna(as_): return ""
        return r["home_team"] if hs > as_ else r["away_team"]
    df["winner"] = df.apply(winner_row, axis=1)

    proj = pd.to_numeric(df["projected_real_run_total"], errors="coerce")
    act = pd.to_numeric(df["actual_real_run_total"], errors="coerce")
    df["run_total_diff"] = (act - proj).where(~act.isna() & ~proj.isna(), pd.NA)

    def fav_ok(r):
        w = str(r.get("winner") or "").strip()
        f = str(r.get("favorite") or "").strip()
        if not w or not f: return ""
        return "Yes" if w.lower() == f.lower() else "No"
    df["favorite_correct"] = df.apply(fav_ok, axis=1)

    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Game bets scored: {out_path}")

if __name__ == "__main__":
    main()
