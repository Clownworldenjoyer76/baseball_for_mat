#!/usr/bin/env python3
# Purpose: ensure every starter in startingpitchers_with_opp_context.csv
# is present in EITHER pitcher_mega_z.csv OR pitcher_props_projected.csv.
# Writes the usual coverage and missing reports; fails only on starters
# absent from BOTH sources.

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUM_DIR = ROOT / "summaries" / "projections"
DATA_DIR = ROOT / "data" / "_projections"
RAW_DIR  = ROOT / "data" / "raw"

SP_WITH_OPP = RAW_DIR / "startingpitchers_with_opp_context.csv"
MEGA_Z = DATA_DIR / "pitcher_mega_z.csv"
PROJ   = DATA_DIR / "pitcher_props_projected.csv"

COV_OUT = SUM_DIR / "mega_z_starter_coverage.csv"
MISS_OUT = SUM_DIR / "mega_z_starter_missing.csv"

def main() -> int:
    SUM_DIR.mkdir(parents=True, exist_ok=True)

    sp = pd.read_csv(SP_WITH_OPP, dtype=str)
    starters = sp["player_id"].dropna().astype(str).unique().tolist()

    mz_ids = set()
    if MEGA_Z.exists():
        mz = pd.read_csv(MEGA_Z, dtype=str)
        pid_col = "player_id" if "player_id" in mz.columns else None
        if pid_col:
            mz_ids = set(mz[pid_col].dropna().astype(str).tolist())

    pr_ids = set()
    if PROJ.exists():
        pr = pd.read_csv(PROJ, dtype=str)
        if "player_id" in pr.columns:
            pr_ids = set(pr["player_id"].dropna().astype(str).tolist())

    have_any = mz_ids.union(pr_ids)
    covered = [pid for pid in starters if pid in have_any]
    missing = [pid for pid in starters if pid not in have_any]

    cov_df = pd.DataFrame({
        "player_id": starters,
        "covered_in_mega_z": [pid in mz_ids for pid in starters],
        "covered_in_projected": [pid in pr_ids for pid in starters],
        "covered_any": [pid in have_any for pid in starters],
    })
    cov_df.to_csv(COV_OUT, index=False)

    miss_df = pd.DataFrame({"player_id": missing})
    miss_df.to_csv(MISS_OUT, index=False)

    if missing:
        msg = f"Starter coverage failure: {len(missing)} starter(s) absent in both sources."
        print(msg)
        raise RuntimeError(msg)
    else:
        print("Starter coverage OK: all starters present in at least one source.")
        return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(1)
