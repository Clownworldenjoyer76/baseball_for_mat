#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pitcher_mega_z.py
- Reads cleaned/normalized pitchers
- Computes mega z-scores or passes-through (as current implementation)
- Writes data/_projections/pitcher_mega_z.csv
"""

from pathlib import Path
import sys
import math
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None

DATA_DIR = Path("data")
CLEANED  = DATA_DIR / "cleaned"
OUTDIR   = DATA_DIR / "_projections"
OUTFILE  = OUTDIR / "pitcher_mega_z.csv"
SOURCE   = CLEANED / "pitchers_normalized_cleaned.csv"

REQ_COLS = ["player_id"]

def _require(df: pd.DataFrame, cols: list, name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise KeyError(f"{name} missing required columns: {miss}")

def _z(s: pd.Series) -> pd.Series:
    mu = s.mean()
    sd = s.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return pd.Series([0]*len(s), index=s.index)
    return (s - mu) / sd

def main() -> int:
    if not SOURCE.exists():
        raise FileNotFoundError(f"Missing input: {SOURCE}")
    OUTDIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(SOURCE)
    df.columns = [c.strip() for c in df.columns]
    _require(df, REQ_COLS, "pitchers_normalized_cleaned")

    # Example: if there are numeric rate columns, compute z-scores (placeholder)
    # Keep original df as "mega"
    mega = df.copy()

    # === ENFORCE STRING IDS ===
    for __c in ["home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id", "player_id", "team_id", "game_id"]:
        if __c in mega.columns:
            mega[__c] = mega[__c].astype("string")
    # === END ENFORCE ===
    mega.to_csv(OUTFILE, index=False)
    print(f"✅ Wrote: {OUTFILE}  (rows={len(mega)})  source={SOURCE}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
