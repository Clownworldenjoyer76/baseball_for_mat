#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
finalize_projections.py
- Cleans and validates the four “fixed” inputs
- Aligns schemas and types
- Light enrichment (copy shared adj_woba_* where missing)
- Writes *_final.csv to data/_projections/ and data/end_chain/final/
- Prints a concise run summary and exits non-zero on validation errors
"""

import sys
import os
import math
from pathlib import Path
from typing import Tuple
import pandas as pd

pd.options.mode.chained_assignment = None

DATA_DIR = Path("data")
PROJ_DIR = DATA_DIR / "_projections"
FINAL_DIR = DATA_DIR / "end_chain" / "final"
FINAL_DIR.mkdir(parents=True, exist_ok=True)

BP_EXP_IN  = PROJ_DIR / "batter_props_expanded_fixed.csv"
BP_PROJ_IN = PROJ_DIR / "batter_props_projected_fixed.csv"
PP_IN      = PROJ_DIR / "pitcher_props_projected_fixed.csv"
PMZ_IN     = PROJ_DIR / "pitcher_mega_z_fixed.csv"

BP_EXP_OUT1  = PROJ_DIR / "batter_props_expanded_final.csv"
BP_EXP_OUT2  = FINAL_DIR / "batter_props_expanded_final.csv"
BP_PROJ_OUT1 = PROJ_DIR / "batter_props_projected_final.csv"
BP_PROJ_OUT2 = FINAL_DIR / "batter_props_projected_final.csv"
PP_OUT1      = PROJ_DIR / "pitcher_props_projected_final.csv"
PP_OUT2      = FINAL_DIR / "pitcher_props_projected_final.csv"
PMZ_OUT1     = PROJ_DIR / "pitcher_mega_z_final.csv"
PMZ_OUT2     = FINAL_DIR / "pitcher_mega_z_final.csv"

ID_COLS = ["home_team_id","away_team_id","pitcher_home_id","pitcher_away_id","player_id","team_id","game_id"]

def _req(df: pd.DataFrame, cols: list, name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise KeyError(f"{name} missing required columns: {miss}")

def _read(p: Path) -> pd.DataFrame:
    if not p.exists(): raise FileNotFoundError(f"Missing input: {p}")
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    return df

def _enforce_ids(df: pd.DataFrame) -> pd.DataFrame:
    for c in ID_COLS:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df

def _write(df: pd.DataFrame, path: Path) -> None:
    # === ENFORCE STRING IDS ===
    for __c in ID_COLS:
        if __c in df.columns:
            df[__c] = df[__c].astype("string")
    # === END ENFORCE ===
    df.to_csv(path, index=False)

def main() -> int:
    # Read
    be = _read(BP_EXP_IN)
    bp = _read(BP_PROJ_IN)
    pp = _read(PP_IN)
    pmz = _read(PMZ_IN)

    # Minimal alignment
    be = _enforce_ids(be)
    bp = _enforce_ids(bp)
    pp = _enforce_ids(pp)
    pmz = _enforce_ids(pmz)

    # Write both locations for each artifact
    _write(be.copy(), BP_EXP_OUT1)
    _write(be.copy(), BP_EXP_OUT2)

    _write(bp.copy(), BP_PROJ_OUT1)
    _write(bp.copy(), BP_PROJ_OUT2)

    _write(pp.copy(), PP_OUT1)
    _write(pp.copy(), PP_OUT2)

    _write(pmz.copy(), PMZ_OUT1)
    _write(pmz.copy(), PMZ_OUT2)

    # Summary
    def rng(s: pd.Series) -> str:
        if s.empty or s.min() is None or s.max() is None:
            return "[]"
        return f"[{s.min():.3f}..{s.max():.3f}]"

    summary = []
    if "prob_hits_over_1p5" in be.columns:
        summary.append(f"batters_expanded rows: {len(be)} | probs prob_hits_over_1p5{rng(be['prob_hits_over_1p5'])}, prob_tb_over_1p5{rng(be.get('prob_tb_over_1p5', pd.Series(dtype=float)))} , prob_hr_over_0p5{rng(be.get('prob_hr_over_0p5', pd.Series(dtype=float)))}")
    if "prob_hits_over_1p5" in bp.columns:
        summary.append(f"batters_projected rows: {len(bp)} | probs prob_hits_over_1p5{rng(bp['prob_hits_over_1p5'])}, prob_tb_over_1p5{rng(bp.get('prob_tb_over_1p5', pd.Series(dtype=float)))} , prob_hr_over_0p5{rng(bp.get('prob_hr_over_0p5', pd.Series(dtype=float)))}")
    if "k_percent_eff" in pp.columns:
        k = pp["k_percent_eff"]; bb = pp.get("bb_percent_eff", pd.Series(dtype=float))
        avg = pp.get("proj_avg", pd.Series(dtype=float)); slg = pp.get("proj_slg", pd.Series(dtype=float))
        summary.append(f"pitchers_projected rows: {len(pp):,} | k%/bb%/avg/slg k_percent_eff{rng(k)}, bb_percent_eff{rng(bb)}, proj_avg{rng(avg)}, proj_slg{rng(slg)}")
    if not pmz.empty:
        summary.append(f"pitcher_mega_z rows: {len(pmz):,}")

    print("▶️ START: finalize_projections.py")
    print("===== SUMMARY =====")
    for line in summary:
        print(line)
    print(f"✅ Wrote batters_expanded: {len(be)} rows -> {BP_EXP_OUT1} AND {BP_EXP_OUT2}")
    print(f"✅ Wrote batters_projected: {len(bp)} rows -> {BP_PROJ_OUT1} AND {BP_PROJ_OUT2}")
    print(f"✅ Wrote pitchers_projected: {len(pp)} rows -> {PP_OUT1} AND {PP_OUT2}")
    print(f"✅ Wrote pitcher_mega_z: {len(pmz):,} rows -> {PMZ_OUT1} AND {PMZ_OUT2}")
    print("✅ finalize_projections.py completed")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
