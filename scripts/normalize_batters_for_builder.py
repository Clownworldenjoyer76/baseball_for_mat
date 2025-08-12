#!/usr/bin/env python3
# scripts/normalize_batters_for_builder.py
# Purpose: ensure data/tagged/batters_normalized.csv has the strict columns required by
# build_expanded_batter_props.py (Mode B). It augments IN PLACE and fails fast if it cannot.

import sys
from pathlib import Path
import pandas as pd
import numpy as np

SRC = Path("data/tagged/batters_normalized.csv")

# Columns the builder requires
REQ = [
    "player_id","name","team",
    "pa","g","ab","slg",
    "season_hits","season_tb","season_hr","season_bb","season_k"
]

# Common alias map (source_col -> target_col)
ALIASES = {
    # volume/opportunity
    "PA": "pa", "plate_appearances": "pa",
    "AB": "ab", "at_bats": "ab",
    "G": "g", "games": "g",
    "SLG": "slg", "xslg": "slg",
    # season totals
    "hits": "season_hits", "total_hits": "season_hits", "proj_hits": "season_hits",
    "b_total_bases": "season_tb", "proj_tb": "season_tb", "total_bases": "season_tb",
    "home_run": "season_hr", "hr": "season_hr", "proj_hr": "season_hr",
    "walk": "season_bb", "walks": "season_bb", "bb": "season_bb",
    "strikeout": "season_k", "k": "season_k", "so": "season_k",
}

NUMERIC = {"pa","g","ab","slg","season_hits","season_tb","season_hr","season_bb","season_k"}

def fail(msg):
    print(f"❌ normalize_batters_for_builder: {msg}", file=sys.stderr)
    sys.exit(1)

def main():
    if not SRC.exists():
        fail(f"Source file missing: {SRC}")
    df = pd.read_csv(SRC)
    df.columns = df.columns.map(str).str.strip()

    # Standardize id/name/team presence early
    for col in ["player_id","name","team"]:
        if col not in df.columns:
            # try simple case-insensitive lookup
            lc_map = {c.lower(): c for c in df.columns}
            if col.lower() in lc_map:
                df.rename(columns={lc_map[col.lower()]: col}, inplace=True)
            else:
                fail(f"Missing identifier column '{col}'")

    # Apply alias renames where targets don't already exist
    for src_col, tgt_col in ALIASES.items():
        if tgt_col not in df.columns and src_col in df.columns:
            df.rename(columns={src_col: tgt_col}, inplace=True)

    # Coerce numerics
    for col in NUMERIC & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derive missing numeric fields deterministically
    # g: games played
    if "g" not in df.columns or df["g"].isna().all():
        if "pa" in df.columns:
            df["g"] = (df["pa"] / 4.2).round().astype("Int64")
        else:
            fail("Cannot derive 'g' (games) without 'pa'")

    # slg: slugging percentage
    if "slg" not in df.columns or df["slg"].isna().all():
        if "season_tb" in df.columns and "ab" in df.columns:
            # avoid division by zero
            df["slg"] = np.where(df["ab"].fillna(0) > 0, df["season_tb"] / df["ab"], 0.0)
        else:
            fail("Cannot derive 'slg' without both 'season_tb' and 'ab'")

    # Ensure the rest exist (at least as columns)
    for col in ["season_hits","season_tb","season_hr","season_bb","season_k","pa","ab"]:
        if col not in df.columns:
            fail(f"Missing required column '{col}' and no alias available to derive it")

    # Final coercion and validation
    for col in NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows that still have fatal NaNs in numeric required columns
    before = len(df)
    df_clean = df.dropna(subset=list(NUMERIC), how="any").copy()
    dropped = before - len(df_clean)

    # Reasonable bounds (guard rails)
    df_clean = df_clean[(df_clean["g"] > 0) & (df_clean["pa"] >= 0) & (df_clean["ab"] >= 0)]
    df_clean["slg"] = df_clean["slg"].clip(lower=0.0, upper=2.0)

    if df_clean.empty:
        fail("All rows invalid after normalization; check inputs.")

    # Write IN PLACE
    df_clean.to_csv(SRC, index=False)
    print(f"✅ normalize_batters_for_builder: wrote {SRC}  (kept {len(df_clean)}/{before}, dropped {dropped})")

if __name__ == "__main__":
    main()
