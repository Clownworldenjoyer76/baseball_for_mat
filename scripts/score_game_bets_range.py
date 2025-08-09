#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import requests
import pandas as pd


# ------------------------
# Helpers
# ------------------------

def _parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def _date_range_for_args(args) -> Tuple[dt.date, dt.date]:
    """Resolve a single date, a start/end (--range or --start/--end), or --since."""
    if getattr(args, "date", None):
        d = _parse_date(args.date)
        return d, d

    # --range YYYY-MM-DD:YYYY-MM-DD
    if getattr(args, "range", None):
        a, b = args.range.split(":")
        return _parse_date(a), _parse_date(b)

    # --start/--end pair
    if getattr(args, "start", None) and getattr(args, "end", None):
        return _parse_date(args.start), _parse_date(args.end)

    # --since YYYY-MM-DD  (until yesterday)
    if getattr(args, "since", None):
        a = _parse_date(args.since)
        b = dt.date.today() - dt.timedelta(days=1)
        return a, b

    # default: yesterday
    y = dt.date.today() - dt.timedelta(days=1)
    return y, y


def _build_schedule_url(base_api: str, start_date: dt.date, end_date: dt.date) -> Tuple[str, Dict[str, Any]]:
    """
    Accepts base like 'https://statsapi.mlb.com/api/v1' OR an existing schedule URL.
    Returns (url, params) where params contains sportId + date or startDate/endDate as required.
    """
    if not base_api:
        raise ValueError("API base is required when not using --results")

    base = base_api.rstrip("/")
    # If caller already passed a schedule endpoint, keep it; else append /schedule
    if not base.endswith("/schedule"):
        if base.endswith("/api/v1"):
            base = f"{base}/schedule"
        else:
            # user gave some other path; leave as-is
            pass

    params: Dict[str, Any] = {"sportId": 1}
    if start_date == end_date:
        params["date"] = start_date.strftime("%Y-%m-%d")
    else:
        params["startDate"] = start_date.strftime("%Y-%m-%d")
        params["endDate"] = end_date.strftime("%Y-%m-%d")

    # common filters to reduce payload noise; safe even if API ignores them
    params.update({
        "hydrate": "team,linescore,decisions,flags,weather",
        "gameType": "R",  # regular season
    })
    return base, params


def _fetch_schedule(api_base: str, start_date: dt.date, end_date: dt.date) -> Dict[str, Any]:
    url, params = _build_schedule_url(api_base, start_date, end_date)
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _rows_from_schedule(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform MLB schedule JSON into minimal rows this scorer expects."""
    rows: List[Dict[str, Any]] = []
    dates = data.get("dates", [])
    for d in dates:
        for g in d.get("games", []):
            home = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
            away = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
            home_score = g.get("teams", {}).get("home", {}).get("score", None)
            away_score = g.get("teams", {}).get("away", {}).get("score", None)
            game_date = g.get("officialDate") or d.get("date")
            rows.append({
                "date": game_date,
                "home_team": home,
                "away_team": away,
                "home_score": home_score,
                "away_score": away_score,
                "game_pk": g.get("gamePk"),
                "status": g.get("status", {}).get("detailedState"),
                "venue": (g.get("venue") or {}).get("name", ""),
            })
    return rows


def _load_results_from_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"Results CSV not found: {p}")
    return pd.read_csv(p)

def _load_results_from_api(api_base: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    data = _fetch_schedule(api_base, start_date, end_date)
    rows = _rows_from_schedule(data)
    return pd.DataFrame(rows)


def _score_game_props(games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal scoring:
      - favorite = team with higher actual total if scores present, else ''
      - actual_real_run_total if scores present
    Keeps prior column names used by your pipeline.
    """
    out = games_df.copy()
    # keep date as YYYY-MM-DD
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")

    # totals if we have scores
    if {"home_score", "away_score"}.issubset(out.columns):
        mask = out["home_score"].notna() & out["away_score"].notna()
        out.loc[mask, "actual_real_run_total"] = (out.loc[mask, "home_score"].astype(float)
                                                  + out.loc[mask, "away_score"].astype(float)).round(2)
    # favorite (if scores known)
    def _fav(row):
        hs, as_ = row.get("home_score"), row.get("away_score")
        if pd.notna(hs) and pd.notna(as_):
            return row["home_team"] if float(hs) > float(as_) else row["away_team"]
        return ""
    out["favorite"] = out.apply(_fav, axis=1)
    return out


# ------------------------
# CLI
# ------------------------

def main():
    parser = argparse.ArgumentParser(description="Score game bets over a date or range.")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--range", help="YYYY-MM-DD:YYYY-MM-DD")
    parser.add_argument("--since", help="YYYY-MM-DD (until yesterday)")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--api", help="MLB Stats API base or schedule endpoint")
    parser.add_argument("--results", help="Optional pre-fetched results CSV instead of API")
    parser.add_argument("--out", default="data/bets/game_props_scored.csv",
                        help="Output CSV (default: data/bets/game_props_scored.csv)")
    args = parser.parse_args()

    start_date, end_date = _date_range_for_args(args)

    # Load results either from API (with auto endpoint + params) or from CSV
    if args.api:
        results_df = _load_results_from_api(args.api, start_date, end_date)
    else:
        if not args.results:
            raise SystemExit("Provide --api or --results")
        results_df = _load_results_from_csv(Path(args.results))

    if results_df.empty:
        print("No games found in the given range.")
        return

    scored = _score_game_props(results_df)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Saved game scoring: {out_path} (rows: {len(scored)})")


if __name__ == "__main__":
    main()
