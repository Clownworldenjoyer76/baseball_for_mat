#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Tuple

import requests
import pandas as pd


# ------------------------
# Helpers
# ------------------------

def _parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def _date_from_args(args) -> Tuple[dt.date, dt.date]:
    if getattr(args, "date", None):
        d = _parse_date(args.date)
        return d, d
    if getattr(args, "start", None) and getattr(args, "end", None):
        return _parse_date(args.start), _parse_date(args.end)
    # default yesterday
    y = dt.date.today() - dt.timedelta(days=1)
    return y, y


def _build_schedule_url(base_api: str, start_date: dt.date, end_date: dt.date) -> Tuple[str, Dict[str, Any]]:
    base = base_api.rstrip("/")
    if not base.endswith("/schedule"):
        if base.endswith("/api/v1"):
            base = f"{base}/schedule"
    params: Dict[str, Any] = {"sportId": 1}
    if start_date == end_date:
        params["date"] = start_date.strftime("%Y-%m-%d")
    else:
        params["startDate"] = start_date.strftime("%Y-%m-%d")
        params["endDate"] = end_date.strftime("%Y-%m-%d")
    # request enough to assemble player scoring context
    params.update({"hydrate": "team,linescore,decisions,flags,weather"})
    return base, params


def _fetch_schedule(api_base: str, start_date: dt.date, end_date: dt.date) -> List[int]:
    """Return list of gamePks for the day/range."""
    url, params = _build_schedule_url(api_base, start_date, end_date)
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    pks: List[int] = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            gp = g.get("gamePk")
            if gp:
                pks.append(int(gp))
    return pks


def _fetch_boxscore(base_api: str, game_pk: int) -> Dict[str, Any]:
    """Build a valid boxscore endpoint from base; fetch JSON."""
    base = base_api.rstrip("/")
    if base.endswith("/schedule"):
        base = base[:-len("/schedule")]
    if base.endswith("/api/v1"):
        url = f"{base}/game/{game_pk}/boxscore"
    else:
        # If a full path was supplied, trust caller and append
        url = f"{base}/game/{game_pk}/boxscore"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def _rows_from_boxscore(game_pk: int, data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract minimal player outcomes. Expand to your exact schema as needed."""
    rows: List[Dict[str, Any]] = []

    teams = data.get("teams", {})
    for side in ("home", "away"):
        t = teams.get(side, {})
        players = (t.get("players") or {})
        for _, p in players.items():
            person = p.get("person") or {}
            batting = (p.get("stats") or {}).get("batting") or {}
            pitching = (p.get("stats") or {}).get("pitching") or {}

            name = person.get("fullName", "")
            team_name = (t.get("team") or {}).get("name", "")

            rows.append({
                "game_pk": game_pk,
                "player_name": name,
                "team": team_name,
                # Useful outcomes for props
                "hits": batting.get("hits"),
                "home_runs": batting.get("homeRuns"),
                "total_bases": batting.get("totalBases"),
                "strikeouts_batter": batting.get("strikeOuts"),
                "walks_batter": batting.get("baseOnBalls"),
                "strikeouts_pitcher": pitching.get("strikeOuts"),
                "walks_pitcher": pitching.get("baseOnBalls"),
                "innings_pitched": pitching.get("inningsPitched"),
            })
    return rows


def _score_player_props(df: pd.DataFrame) -> pd.DataFrame:
    """
    Placeholder scorer that just passes through outcomes.
    Merge this against your prop picks to set prop_correct, etc.
    """
    out = df.copy()
    return out


# ------------------------
# CLI
# ------------------------

def main():
    parser = argparse.ArgumentParser(description="Score player bets for a date/range.")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--api", required=True, help="MLB Stats API base or schedule endpoint")
    parser.add_argument("--out", default="data/bets/player_props_scored.csv",
                        help="Output CSV (default: data/bets/player_props_scored.csv)")
    args = parser.parse_args()

    start_date, end_date = _date_from_args(args)

    # 1) find games for the day from schedule (auto constructs params)
    game_pks = _fetch_schedule(args.api, start_date, end_date)
    if not game_pks:
        print("No games found; nothing to score.")
        return

    # 2) pull boxscore per game and collect rows
    all_rows: List[Dict[str, Any]] = []
    for gp in game_pks:
        try:
            box = _fetch_boxscore(args.api, gp)
            all_rows.extend(_rows_from_boxscore(gp, box))
        except requests.HTTPError as e:
            print(f"Warning: boxscore for {gp} failed: {e}")

    if not all_rows:
        print("No player rows collected.")
        return

    df = pd.DataFrame(all_rows)
    scored = _score_player_props(df)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Saved player scoring: {out_path} (rows: {len(scored)})")


if __name__ == "__main__":
    main()
