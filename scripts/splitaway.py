#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Split batters by AWAY side using IDs only.

INPUTS
- data/cleaned/batters_today.csv            (must contain: team_id)
- data/raw/todaysgames_normalized.csv       (must contain: home_team_id, away_team_id, game_time, park_factor)

OUTPUT
- data/adjusted/batters_away.csv            (all batter cols + side/opponent/game context)

Notes:
- Joins strictly on IDs. No team names/abbreviations are referenced or created.
"""

from pathlib import Path
import pandas as pd
import sys

BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
TODAYS_GAMES_FILE  = "data/raw/todaysgames_normalized.csv"
OUTPUT_FILE        = "data/adjusted/batters_away.csv"

REQUIRED_BATTER_COLS = {"team_id"}
REQUIRED_GAME_COLS   = {"home_team_id", "away_team_id", "game_time", "park_factor"}


def main(batters_path: str = BATTERS_TODAY_FILE,
         games_path: str = TODAYS_GAMES_FILE,
         out_path: str = OUTPUT_FILE) -> None:

    print(f"--- splitaway.py (IDs only) ---")
    print(f"batters: {batters_path}")
    print(f"games:   {games_path}")
    print(f"output:  {out_path}")

    # Load inputs
    try:
        bat = pd.read_csv(batters_path)
    except Exception as e:
        print(f"INSUFFICIENT INFORMATION: cannot read {batters_path} ({e})"); return

    try:
        g = pd.read_csv(games_path)
    except Exception as e:
        print(f"INSUFFICIENT INFORMATION: cannot read {games_path} ({e})"); return

    # Validate required columns
    if not REQUIRED_BATTER_COLS.issubset(bat.columns):
        missing = REQUIRED_BATTER_COLS - set(bat.columns)
        print(f"INSUFFICIENT INFORMATION: {batters_path} missing columns: {sorted(missing)}"); return

    if not REQUIRED_GAME_COLS.issubset(g.columns):
        missing = REQUIRED_GAME_COLS - set(g.columns)
        print(f"INSUFFICIENT INFORMATION: {games_path} missing columns: {sorted(missing)}"); return

    # Prepare away index (IDs only)
    away_ids = g["away_team_id"].dropna().astype("Int64").unique()
    print(f"away_team_id count: {len(away_ids)}")

    # Filter batters whose team_id is an away ID
    # Ensure consistent dtype for safe matching
    if bat["team_id"].dtype.name != "Int64":
        # try converting; if fails, error out cleanly
        try:
            bat["team_id"] = bat["team_id"].astype("Int64")
        except Exception as e:
            print(f"INSUFFICIENT INFORMATION: team_id in {batters_path} not convertible to integer ({e})")
            return

    away_bat = bat[bat["team_id"].isin(away_ids)].copy()

    # Join minimal game context (IDs only)
    # Merge on away_team_id == team_id
    ctx_cols = ["away_team_id", "home_team_id", "game_time", "park_factor"]
    g_ctx = g[ctx_cols].copy()

    merged = away_bat.merge(
        g_ctx,
        left_on="team_id",
        right_on="away_team_id",
        how="left",
        validate="m:1"
    )

    # Add normalized side/opponent columns (IDs only)
    merged.insert(len(merged.columns), "side", "away")
    merged.insert(len(merged.columns), "opponent_team_id", merged["home_team_id"])

    # Reorder: keep original batter cols first, then ID-only game context
    batter_cols = [c for c in bat.columns]
    extra_cols = ["side", "opponent_team_id", "home_team_id", "away_team_id", "game_time", "park_factor"]
    ordered_cols = batter_cols + [c for c in extra_cols if c not in batter_cols]
    merged = merged[ordered_cols]

    # Ensure output dir
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    # If empty, still emit headers
    merged.to_csv(out_path, index=False)
    print(f"wrote {out_path} rows={len(merged)} (IDs only)")
    print(f"--- done ---")


if __name__ == "__main__":
    # Optional CLI override: python scripts/splitaway.py [batters_csv] [games_csv] [out_csv]
    if len(sys.argv) >= 2:
        bat_path = sys.argv[1]
        games_path = sys.argv[2] if len(sys.argv) >= 3 else TODAYS_GAMES_FILE
        out_path = sys.argv[3] if len(sys.argv) >= 4 else OUTPUT_FILE
        main(bat_path, games_path, out_path)
    else:
        main()
