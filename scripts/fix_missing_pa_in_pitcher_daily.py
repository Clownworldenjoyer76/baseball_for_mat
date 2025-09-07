#!/usr/bin/env python3
# scripts/fix_missing_pa_in_pitcher_daily.py
#
# Purpose:
#   Ensure data/_projections/pitcher_props_projected_final.csv contains a numeric 'pa' column.
#   If missing or null, fill from season-level data in data/Data/pitchers.csv.
#   Output overwrites the same daily file in place.

import pandas as pd
from pathlib import Path

PITCHERS_DAILY_IN  = Path("data/_projections/pitcher_props_projected_final.csv")
PITCHERS_SEASON    = Path("data/Data/pitchers.csv")
PITCHERS_DAILY_OUT = Path("data/_projections/pitcher_props_projected_final.csv")

def main():
    pit_d = pd.read_csv(PITCHERS_DAILY_IN)
    pit_s = pd.read_csv(PITCHERS_SEASON)

    need_daily = ["player_id","game_id","team_id","opponent_team_id"]
    miss_daily = [c for c in need_daily if c not in pit_d.columns]
    if miss_daily:
        raise RuntimeError(f"{PITCHERS_DAILY_IN} missing columns: {miss_daily}")

    need_season = ["player_id","pa"]
    miss_season = [c for c in need_season if c not in pit_s.columns]
    if miss_season:
        raise RuntimeError(f"{PITCHERS_SEASON} missing columns: {miss_season}")

    pit_s = pit_s[["player_id","pa"]].copy()
    pit_s["pa"] = pd.to_numeric(pit_s["pa"], errors="coerce").fillna(0.0)

    if "pa" not in pit_d.columns:
        pit_d["pa"] = pd.NA
    pit_d["pa"] = pd.to_numeric(pit_d["pa"], errors="coerce")

    pit_d = pit_d.merge(pit_s, on="player_id", how="left", suffixes=("", "_season"))
    pit_d["pa"] = pit_d["pa"].fillna(pit_d["pa_season"]).fillna(0.0)
    pit_d.drop(columns=[c for c in pit_d.columns if c.endswith("_season")], inplace=True)

    pit_d["pa"] = pd.to_numeric(pit_d["pa"], errors="coerce").fillna(0.0).clip(lower=0)

    pit_d.to_csv(PITCHERS_DAILY_OUT, index=False)

if __name__ == "__main__":
    main()
