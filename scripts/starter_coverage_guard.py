#!/usr/bin/env python3
"""
starter_coverage_guard.py

Purpose
-------
Verify that all expected starters for today exist in pitcher_mega_z,
and ALWAYS dump two artifacts (even on failure):

  - summaries/projections/mega_z_starter_coverage.csv
  - summaries/projections/mega_z_starter_missing.csv

Exit code 1 if any starters are missing; 0 otherwise.

How it finds starters (first existing source wins)
--------------------------------------------------
1) data/raw/startingpitchers_with_opp_context.csv      -> column: player_id
2) data/_projections/pitcher_props_projected.csv       -> column: player_id
3) data/_projections/pitcher_props_projected_final.csv -> column: player_id
4) data/_projections/todaysgames_normalized_fixed.csv  -> columns: pitcher_home_id, pitcher_away_id

Where it looks for mega_z
-------------------------
1) data/_projections/pitcher_mega_z.csv
2) data/_projections/pitcher_mega_z_final.csv

All IDs are coerced to stripped strings for comparison.
"""

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
SUM_DIR  = Path("summaries") / "projections"
SUM_DIR.mkdir(parents=True, exist_ok=True)

def _to_str_series(s: pd.Series) -> pd.Series:
    # robust string normalize: None/NaN -> "", strip spaces
    return s.astype(str).str.strip()

def _load_first_existing(candidates: list[tuple[Path, list[str]]]) -> tuple[pd.DataFrame, Path, list[str]]:
    """Return (df, path, cols) for the first existing candidate file."""
    for p, cols in candidates:
        if p.exists():
            df = pd.read_csv(p)
            return df, p, cols
    raise FileNotFoundError(
        "None of the starter sources exist. Checked: "
        + ", ".join(str(p) for p, _ in candidates)
    )

def _pick_megaz_path() -> Path:
    cands = [
        DATA_DIR / "_projections" / "pitcher_mega_z.csv",
        DATA_DIR / "_projections" / "pitcher_mega_z_final.csv",
    ]
    for p in cands:
        if p.exists():
            return p
    raise FileNotFoundError(
        "Could not find pitcher_mega_z file. Checked: "
        + ", ".join(str(p) for p in cands)
    )

def _collect_starter_ids(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Return a Series of unique starter player_ids (as strings) from df using cols."""
    # For todaysgames we may get pitcher_home_id/pitcher_away_id
    if len(cols) == 1:
        col = cols[0]
        if col not in df.columns:
            raise KeyError(f"Starter source missing required column: {col}")
        ids = _to_str_series(df[col])
    else:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise KeyError(f"Starter source missing required columns: {missing}")
        parts = [_to_str_series(df[c]) for c in cols]
        ids = pd.concat(parts, ignore_index=True)

    # Drop empties and duplicates
    ids = ids[ids != ""].dropna().drop_duplicates().reset_index(drop=True)
    return ids

def main() -> None:
    # 1) Determine the starters source (first existing)
    starter_sources = [
        (DATA_DIR / "raw" / "startingpitchers_with_opp_context.csv", ["player_id"]),
        (DATA_DIR / "_projections" / "pitcher_props_projected.csv", ["player_id"]),
        (DATA_DIR / "_projections" / "pitcher_props_projected_final.csv", ["player_id"]),
        (DATA_DIR / "_projections" / "todaysgames_normalized_fixed.csv", ["pitcher_home_id", "pitcher_away_id"]),
    ]
    starters_df, starters_path, starter_cols = _load_first_existing(starter_sources)
    starter_ids = _collect_starter_ids(starters_df, starter_cols)

    # 2) Load mega_z
    megaz_path = _pick_megaz_path()
    megaz_df = pd.read_csv(megaz_path)

    # Try to identify which column in mega_z is the player id
    # Common names: 'player_id', 'mlb_id', 'id'
    cand_cols = [c for c in ["player_id", "mlb_id", "id"] if c in megaz_df.columns]
    if not cand_cols:
        # Fallback: if there's exactly one integer-like ID column, try that
        # but always coerce to string for comparison.
        raise KeyError(
            f"{megaz_path} does not contain a recognizable player id column "
            "(expected one of: player_id, mlb_id, id)."
        )
    megaz_id_col = cand_cols[0]
    megaz_ids = _to_str_series(megaz_df[megaz_id_col]).dropna().drop_duplicates()

    # 3) Compare
    starters_set = set(starter_ids.tolist())
    megaz_set    = set(megaz_ids.tolist())
    missing_ids  = sorted(starters_set - megaz_set)

    # 4) Write artifacts (ALWAYS)
    coverage = pd.DataFrame({
        "player_id": sorted(starters_set),
        "present_in_mega_z": [pid in megaz_set for pid in sorted(starters_set)],
        "starters_source": str(starters_path),
        "mega_z_file": str(megaz_path),
    })
    coverage_out = SUM_DIR / "mega_z_starter_coverage.csv"
    coverage.to_csv(coverage_out, index=False)

    missing_df = pd.DataFrame({"player_id": missing_ids})
    missing_out = SUM_DIR / "mega_z_starter_missing.csv"
    missing_df.to_csv(missing_out, index=False)

    # 5) Console summary
    print("=== starter_coverage_guard summary ===")
    print(f"Starters source : {starters_path}")
    print(f"Mega Z file     : {megaz_path}")
    print(f"Total starters  : {len(starters_set)}")
    print(f"Found in mega_z : {len(starters_set) - len(missing_ids)}")
    print(f"Missing         : {len(missing_ids)}")
    if missing_ids:
        print("Missing player_id(s):")
        for pid in missing_ids:
            print(f"  - {pid}")
        print(f"\nWrote coverage -> {coverage_out}")
        print(f"Wrote missing  -> {missing_out}")
        # Exit non-zero to fail the job (by design)
        raise SystemExit(1)
    else:
        print("All starters present in pitcher_mega_z.")
        print(f"Wrote coverage -> {coverage_out}")
        print(f"Wrote missing  -> {missing_out}")
        raise SystemExit(0)

if __name__ == "__main__":
    main()
