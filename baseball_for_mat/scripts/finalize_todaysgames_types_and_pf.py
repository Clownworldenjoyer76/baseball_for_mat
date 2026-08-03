#!/usr/bin/env python3
# scripts/finalize_todaysgames_types_and_pf.py
#
# Ensures clean data/raw/todaysgames_normalized.csv
# - Coerces ID columns to pandas nullable Int64
# - Coerces park_factor to float
# - Drops helper columns if present (pf_day, pf_night, pf_roof, _hour24)
# - Fails if any row with home_team_id has missing park_factor
# - Preserves and orders anchor columns first, then original order for the rest

from pathlib import Path
import pandas as pd
import sys

ROOT = Path(".")
GAMES_CSV = ROOT / "data/raw/todaysgames_normalized.csv"

HELPER_COLS = ["pf_day", "pf_night", "pf_roof", "_hour24"]

INT_COLS = [
    "game_id",
    "home_team_id",
    "away_team_id",
    "pitcher_home_id",
    "pitcher_away_id",
]

ANCHORS = [
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

def _die(msg: str):
    print(f"INSUFFICIENT INFORMATION\n{msg}")
    sys.exit(1)

def _to_int64_if_present(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

def _to_float_if_present(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

def main():
    if not GAMES_CSV.exists():
        _die(f"Missing file: {GAMES_CSV}")

    df = pd.read_csv(GAMES_CSV)

    # Drop helpers if present
    df = df.drop(columns=[c for c in HELPER_COLS if c in df.columns], errors="ignore")

    # Coerce ID columns
    for c in INT_COLS:
        _to_int64_if_present(df, c)

    # park_factor to float, must exist and be non-null if we have home_team_id
    if "home_team_id" in df.columns:
        if "park_factor" not in df.columns:
            _die("Missing column: park_factor")
        _to_float_if_present(df, "park_factor")
        if df["park_factor"].isna().any():
            bad = df[df["park_factor"].isna()]
            print(bad.to_string(index=False))
            _die("park_factor missing for some rows where home_team_id exists.")

    # Column order: anchors first (only those present), then remaining in original order
    original_order = list(df.columns)
    anchors_present = [c for c in ANCHORS if c in df.columns]
    remaining = [c for c in original_order if c not in anchors_present]
    out = df.loc[:, anchors_present + remaining]

    # Save
    GAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(GAMES_CSV, index=False)

    pf_dtype = out["park_factor"].dtype if "park_factor" in out.columns else "N/A"
    print(f"✅ Wrote {GAMES_CSV} | rows={len(out)} | dtype(park_factor)={pf_dtype}")

if __name__ == "__main__":
    main()
