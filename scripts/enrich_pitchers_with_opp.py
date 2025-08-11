#!/usr/bin/env python3
"""
enrich_pitchers_with_opp.py

Purpose
-------
Augment a pitcher-centric CSV with:
  - opponent_pitcher_id
  - opp_K% (team batting strikeout rate)
  - opp_BB% (team batting walk rate)

Sources: MLB Stats API.
This version ALWAYS writes to data/raw/startingpitchers_with_opp_context.csv by default
and will overwrite that file if it already exists.

Inputs
------
- --infile: path to startingpitchers_final.csv (pitcher base table)
- --date:   game date (YYYY-MM-DD) to query schedule/probable pitchers
- --outfile: optional custom output path (will also overwrite if exists)

Usage
-----
python scripts/enrich_pitchers_with_opp.py --infile data/end_chain/final/startingpitchers_final.csv --date 2025-08-11
"""
import argparse, sys
from pathlib import Path
from typing import Dict, Any, Tuple
import pandas as pd
import requests

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
TEAMS_URL    = "https://statsapi.mlb.com/api/v1/teams?sportId=1&activeStatus=Yes"
TEAM_STATS_TPL = "https://statsapi.mlb.com/api/v1/teams/{teamId}/stats?stats=season&group=hitting"

def _norm(s: str) -> str:
    return "".join(ch.lower() for ch in (s or "").strip())

def fetch_teams() -> pd.DataFrame:
    r = requests.get(TEAMS_URL, timeout=30)
    r.raise_for_status()
    data = r.json().get("teams", [])
    rows = []
    for t in data:
        rows.append({
            "teamId": t.get("id"),
            "name": t.get("name"),
            "teamName": t.get("teamName"),
            "abbreviation": t.get("abbreviation"),
        })
    return pd.DataFrame(rows)

def fetch_schedule(date_str: str) -> pd.DataFrame:
    params = {"sportId": 1, "date": date_str, "hydrate": "probablePitcher"}
    r = requests.get(SCHEDULE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    games = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            home = g.get("teams", {}).get("home", {})
            away = g.get("teams", {}).get("away", {})
            games.append({
                "gamePk": g.get("gamePk"),
                "date": date_str,
                "home_team_id": home.get("team", {}).get("id"),
                "home_team_name": home.get("team", {}).get("name"),
                "away_team_id": away.get("team", {}).get("id"),
                "away_team_name": away.get("team", {}).get("name"),
                "home_prob_pitcher_id": home.get("probablePitcher", {}).get("id"),
                "home_prob_pitcher_fullName": home.get("probablePitcher", {}).get("fullName"),
                "away_prob_pitcher_id": away.get("probablePitcher", {}).get("id"),
                "away_prob_pitcher_fullName": away.get("probablePitcher", {}).get("fullName"),
            })
    return pd.DataFrame(games)

def fetch_team_hitting_rates(team_id: int) -> Tuple[float, float]:
    """Return (K%, BB%) for the team (season-to-date). If missing, returns (None, None)."""
    try:
        r = requests.get(TEAM_STATS_TPL.format(teamId=team_id), timeout=30)
        r.raise_for_status()
        stats = r.json().get("stats", [])
        if not stats or not stats[0].get("splits"):
            return (None, None)
        split = stats[0]["splits"][0]["stat"]
        pa = split.get("plateAppearances")
        so = split.get("strikeOuts")
        bb = split.get("baseOnBalls")
        if not pa or pa == 0:
            return (None, None)
        return (so / pa, bb / pa)
    except Exception:
        return (None, None)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="Path to startingpitchers_final.csv")
    ap.add_argument("--date", required=True, help="Game date YYYY-MM-DD")
    ap.add_argument("--outfile", default=None, help="Optional output CSV path (will overwrite if exists)")
    args = ap.parse_args()

    infile = Path(args.infile)
    if not infile.exists():
        print(f"ERROR: infile not found: {infile}", file=sys.stderr)
        sys.exit(1)

    try:
        base = pd.read_csv(infile)
    except Exception as e:
        print(f"ERROR: failed to read infile: {e}", file=sys.stderr)
        sys.exit(1)

    # Fetch teams and schedule
    teams_df = fetch_teams()
    sched_df = fetch_schedule(args.date)

    if teams_df.empty or sched_df.empty:
        print("WARN: Could not fetch teams or schedule; output will not include opponent fields.", file=sys.stderr)

    # Build team lookup
    team_lookup: Dict[str, int] = {}
    for _, row in teams_df.iterrows():
        for key in filter(None, [row["name"], row["teamName"], row["abbreviation"]]):
            team_lookup[_norm(key)] = int(row["teamId"])

    # Map team -> opponent info for the date
    opp_map: Dict[int, Dict[str, Any]] = {}
    for _, g in sched_df.iterrows():
        home_id = int(g["home_team_id"])
        away_id = int(g["away_team_id"])
        opp_map[home_id] = {
            "opponent_team_id": away_id,
            "opponent_pitcher_id": int(g["away_prob_pitcher_id"]) if pd.notna(g["away_prob_pitcher_id"]) else None
        }
        opp_map[away_id] = {
            "opponent_team_id": home_id,
            "opponent_pitcher_id": int(g["home_prob_pitcher_id"]) if pd.notna(g["home_prob_pitcher_id"]) else None
        }

    # Detect a team column
    team_col = None
    for c in base.columns:
        if _norm(c) in {"team", "playerteam", "mlbteam", "player_team", "mlb_team"}:
            team_col = c
            break

    # Prepare output columns
    base = base.copy()
    base["opponent_pitcher_id"] = None
    base["opponent_team_id"] = None
    base["opp_K%"] = None
    base["opp_BB%"] = None

    # name fallback
    def _full_name(row):
        first = None
        last = None
        for c in base.columns:
            if _norm(c) == "first_name":
                first = str(row[c])
            elif _norm(c) == "last_name":
                last = str(row[c])
        full = f"{first or ''} {last or ''}".strip()
        return full if full else None

    # Build name -> team lookup from schedule probable pitchers as fallback
    name_to_team = {}
    for _, g in sched_df.iterrows():
        if pd.notna(g["home_prob_pitcher_fullName"]):
            name_to_team[_norm(g["home_prob_pitcher_fullName"])] = int(g["home_team_id"])
        if pd.notna(g["away_prob_pitcher_fullName"]):
            name_to_team[_norm(g["away_prob_pitcher_fullName"])] = int(g["away_team_id"])

    # Enrich each row
    rate_cache: Dict[int, Tuple[float, float]] = {}
    for idx, row in base.iterrows():
        team_id = None
        if team_col:
            team_id = team_lookup.get(_norm(str(row[team_col])))

        if team_id is None:
            fn = _full_name(row)
            if fn:
                team_id = name_to_team.get(_norm(fn))

        if team_id is None:
            continue

        info = opp_map.get(team_id, {})
        base.at[idx, "opponent_team_id"] = info.get("opponent_team_id")
        base.at[idx, "opponent_pitcher_id"] = info.get("opponent_pitcher_id")

        opp_team_id = info.get("opponent_team_id")
        if opp_team_id is None:
            continue

        # fetch or reuse opponent team batting rates
        if opp_team_id not in rate_cache:
            k_rate, bb_rate = fetch_team_hitting_rates(opp_team_id)
            rate_cache[opp_team_id] = (k_rate, bb_rate)
        k_rate, bb_rate = rate_cache[opp_team_id]
        base.at[idx, "opp_K%"] = k_rate
        base.at[idx, "opp_BB%"] = bb_rate

    # Output path: always overwrite
    out_path = Path(args.outfile) if args.outfile else Path("data/raw/startingpitchers_with_opp_context.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}  (rows={len(base)})")

if __name__ == "__main__":
    main()
