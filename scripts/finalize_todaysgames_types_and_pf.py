#!/usr/bin/env python3
# Ensure clean data/raw/todaysgames_normalized.csv
# - IDs as Int64 (no .0 on write)
# - park_factor as Int64 (no .0 on write)
# - Drop helper cols (pf_day, pf_night, pf_roof, _hour24)
# - Fail if any row has home_team_id but missing park_factor
# - PRESERVE passthrough columns, especially pitcher_home_id and pitcher_away_id

from pathlib import Path
import pandas as pd
import sys

ROOT = Path(".")
GAMES_CSV = ROOT / "data/raw/todaysgames_normalized.csv"

# Helper columns that may have been added upstream and should be removed here if present.
HELPER_COLS = ["pf_day", "pf_night", "pf_roof", "_hour24"]

# Columns that should be integers (stored as pandas nullable Int64 so they serialize without .0)
INT_COLS = [
    "game_id",
    "home_team_id",
    "away_team_id",
    "pitcher_home_id",
    "pitcher_away_id",
    "park_factor",
]

# Base display/order anchors that should appear first if present
BASE_ANCHOR = [
    "game_id",
    "home_team",
    "away_team",
    "game_time",
    "pitcher_home",
    "pitcher_away",
    "home_team_id",
    "away_team_id",
    "pitcher_home_id",
    "pitcher_away_id",
    "park_factor",
]

def to_int64_if_present(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

def main():
    if not GAMES_CSV.exists():
        print(f"INSUFFICIENT INFORMATION\nMissing file: {GAMES_CSV}")
        sys.exit(1)

    df = pd.read_csv(GAMES_CSV)

    # Drop known helper columns if present
    df = df.drop(columns=[c for c in HELPER_COLS if c in df.columns], errors="ignore")

    # Coerce integer-like columns to Int64 if present
    for c in INT_COLS:
        to_int64_if_present(df, c)

    # Validation: if we have home_team_id, we must have park_factor non-null
    if "home_team_id" in df.columns:
        if "park_factor" not in df.columns:
            print("INSUFFICIENT INFORMATION\nMissing column: park_factor")
            sys.exit(1)
        if df["park_factor"].isna().any():
            bad = df[df["park_factor"].isna()]
            print("INSUFFICIENT INFORMATION\npark_factor missing for some rows where home_team_id exists.")
            print(bad.to_string(index=False))
            sys.exit(1)

    # Build output column order:
    # 1) Start with the original order (preserves any extra/pass-through columns)
    original_order = list(df.columns)

    # 2) Ensure anchor columns appear first, **but only those that actually exist**
    anchors_present = [c for c in BASE_ANCHOR if c in df.columns]

    # 3) Add remaining columns in their original order, excluding those already placed
    remaining = [c for c in original_order if c not in anchors_present]
    ordered_cols = anchors_present + remaining

    # Materialize final frame in that order
    out = df.loc[:, ordered_cols]

    # Save
    GAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(GAMES_CSV, index=False)

    dtype_pf = out["park_factor"].dtype if "park_factor" in out.columns else "N/A"
    print(f"✅ Wrote {GAMES_CSV} | rows={len(out)} | dtype(park_factor)={dtype_pf}")

if __name__ == "__main__":
    main()
