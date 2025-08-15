#!/usr/bin/env python3
# scripts/final_scores_1.py
#
# Purpose: Write a pure game-level table (one row per game) to
#          data/bets/game_props_history.csv (games only).

import pandas as pd
from pathlib import Path

SCHED_FILE = Path("data/bets/mlb_sched.csv")
GAME_OUT   = Path("data/bets/game_props_history.csv")

def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df

def _games_only_from_schedule(sched: pd.DataFrame) -> pd.DataFrame:
    """
    Produce the GAME list (no players) directly from the schedule.
    Keeps common columns if present; fails if required base columns are missing.
    """
    base_cols = ["game_id", "date", "home_team", "away_team"]
    optional_cols = [
        "start_time", "status", "venue",
        "home_probable_pitcher", "away_probable_pitcher",
        "home_pitcher", "away_pitcher",
        "home_moneyline", "away_moneyline",
        "total", "spread", "home_score", "away_score"
    ]
    have = [c for c in base_cols if c in sched.columns]
    if len(have) < 4:
        missing = [c for c in base_cols if c not in sched.columns]
        raise SystemExit(f"❌ schedule missing required columns for games output: {missing}")

    keep = have + [c for c in optional_cols if c in sched.columns]
    games = sched[keep].drop_duplicates().copy()

    # Normalize a ‘matchup’ column
    games["matchup"] = (
        games["away_team"].astype(str).str.strip()
        + " @ "
        + games["home_team"].astype(str).str.strip()
    )

    # Order by date then game_id if present
    sort_cols = [c for c in ["date", "game_id"] if c in games.columns]
    if sort_cols:
        games = games.sort_values(sort_cols, ascending=True)

    return games

def main():
    if not SCHED_FILE.exists():
        raise SystemExit(f"❌ Missing {SCHED_FILE}")

    sched = _std(pd.read_csv(SCHED_FILE))
    games_out = _games_only_from_schedule(sched)

    GAME_OUT.parent.mkdir(parents=True, exist_ok=True)
    games_out.to_csv(GAME_OUT, index=False)

    print(f"✅ Wrote games → {GAME_OUT} (rows={len(games_out)})")

if __name__ == "__main__":
    main()
