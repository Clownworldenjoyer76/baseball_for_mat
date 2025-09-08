#!/usr/bin/env python3
"""
clean_pitcher_files.py
- Do NOT drop rows for NaNs.
- Normalize player_id (string, no trailing .0) and game_id types.
- Preserve all current starters from todaysgames_normalized_fixed.csv and startingpitchers.csv.
- Deduplicate only if necessary, prioritizing starters, then non-null stats.
- Write files in-place and emit a small summary.
"""

from pathlib import Path
import pandas as pd
import numpy as np

# Inputs we clean in-place
TARGETS = [
    Path("data/_projections/pitcher_props_projected_final.csv"),
    Path("data/_projections/pitcher_mega_z_final.csv"),
]

# Starter sources
TODAY = Path("data/_projections/todaysgames_normalized_fixed.csv")
STARTERS = Path("data/end_chain/final/startingpitchers.csv")

SUM_DIR = Path("summaries/projections")
SUM_DIR.mkdir(parents=True, exist_ok=True)

def as_str_id(x):
    """Coerce IDs to clean strings: '605540.0' -> '605540', keep empty as ''."""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s

def load_starter_ids() -> set:
    ids = set()

    # From todaysgames_normalized_fixed.csv
    if TODAY.exists():
        tg = pd.read_csv(TODAY)
        for col in ["pitcher_home_id", "pitcher_away_id"]:
            if col in tg.columns:
                ids |= {as_str_id(v) for v in tg[col].tolist() if as_str_id(v)}

    # From startingpitchers.csv
    if STARTERS.exists():
        sp = pd.read_csv(STARTERS)
        # Accept common column names
        cand_cols = [c for c in sp.columns if c.lower() in {"player_id","pitcher_id","id"}]
        if cand_cols:
            col = cand_cols[0]
            ids |= {as_str_id(v) for v in sp[col].tolist() if as_str_id(v)}

    return ids

def reorder_columns(df: pd.DataFrame, original_cols: list) -> pd.DataFrame:
    """Keep original column order if possible; append any new columns at the end."""
    keep = [c for c in original_cols if c in df.columns]
    tail = [c for c in df.columns if c not in keep]
    return df[keep + tail]

def safe_numeric(series, dtype="Int64"):
    """Best-effort numeric cast that preserves NaNs."""
    out = pd.to_numeric(series, errors="coerce")
    if dtype == "Int64":
        return out.astype("Int64")
    return out

def clean_file(path: Path, starter_ids: set):
    if not path.exists():
        print(f"⚠️  Missing file: {path}")
        return

    df = pd.read_csv(path)
    original_cols = list(df.columns)

    # Normalize IDs
    if "player_id" in df.columns:
        df["player_id"] = df["player_id"].apply(as_str_id)
    if "game_id" in df.columns:
        # keep as integer-like but allow NaN
        df["game_id"] = safe_numeric(df["game_id"], dtype="Int64")

    # Mark starters
    df["__is_starter__"] = df.get("player_id", "").isin(starter_ids)

    # Avoid destructive drops. Only dedupe if true duplicates exist.
    # Define a key for dedupe preference:
    #   1) keep starters first
    #   2) keep rows with more non-null stats
    #   3) stable order otherwise
    non_stat_cols = {"player_id","game_id","team_id","opponent_team_id","role","name","team",
                     "city","state","timezone","is_dome","park_factor"}
    stat_cols = [c for c in df.columns if c not in non_stat_cols and not c.startswith("__")]

    df["__nnz__"] = df[stat_cols].notna().sum(axis=1)

    if "player_id" in df.columns and "game_id" in df.columns:
        # Deduplicate on (player_id, game_id)
        df = (df
              .sort_values(["__is_starter__", "__nnz__"], ascending=[False, False])
              .drop_duplicates(subset=["player_id","game_id"], keep="first")
              .sort_index())
    elif "player_id" in df.columns:
        # Fallback: dedupe on player_id only (still prioritize starters)
        df = (df
              .sort_values(["__is_starter__", "__nnz__"], ascending=[False, False])
              .drop_duplicates(subset=["player_id"], keep="first")
              .sort_index())

    # Remove helper cols
    df = df.drop(columns=[c for c in ["__is_starter__","__nnz__"] if c in df.columns])

    # Restore column order as best as possible
    df = reorder_columns(df, original_cols)

    # Write back
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

    # Starter coverage summary (just to be explicit)
    starters_df = pd.DataFrame({"player_id": sorted(list(starter_ids))})
    starters_df["in_file"] = starters_df["player_id"].isin(set(df.get("player_id","")))
    miss = starters_df[~starters_df["in_file"]]

    sum_file = SUM_DIR / f"{path.stem}_starter_coverage.csv"
    starters_df.to_csv(sum_file, index=False)

    miss_file = SUM_DIR / f"{path.stem}_starter_missing.csv"
    miss.to_csv(miss_file, index=False)

    print(f"✅ cleaned {path} | rows={len(df)} | starters missing here={len(miss)}")
    if len(miss):
        # Also print the list inline for the Action logs
        print("Missing starter ids in this file:", ", ".join(miss["player_id"].tolist()))

def main():
    starter_ids = load_starter_ids()
    print(f"Starters seen today: {len(starter_ids)}")

    for p in TARGETS:
        clean_file(p, starter_ids)

if __name__ == "__main__":
    main()
