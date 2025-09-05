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
from typing import Dict, List, Tuple
import pandas as pd

pd.options.mode.chained_assignment = None

# ---------- Config ----------
INPUTS = {
    "batters_expanded": "data/_projections/batter_props_expanded_fixed.csv",
    "batters_projected": "data/_projections/batter_props_projected_fixed.csv",
    "pitchers_projected": "data/_projections/pitcher_props_projected_fixed.csv",
    "pitcher_mega_z": "data/_projections/pitcher_mega_z_fixed.csv",   # if absent, we skip but log
}

OUT_DIR_A = Path("data/_projections")
OUT_DIR_B = Path("data/end_chain/final")

OUTFILES = {
    "batters_expanded": "batter_props_expanded_final.csv",
    "batters_projected": "batter_props_projected_final.csv",
    "pitchers_projected": "pitcher_props_projected_final.csv",
    "pitcher_mega_z": "pitcher_mega_z_final.csv",
}

# Core columns we care about
BATTER_KEY = ["player_id"]  # game_id is optional in inputs; keep if present
BATTER_PROJ_CORE = [
    "proj_pa_used", "proj_ab_est", "proj_avg_used", "proj_iso_used", "proj_hr_rate_pa_used"
]
BATTER_PROB_COLS = ["prob_hits_over_1p5", "prob_tb_over_1p5", "prob_hr_over_0p5"]
BATTER_WOBA_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

PITCHER_KEY = ["player_id"]  # role/game_id may exist; preserve if present
PITCHER_PROJ_CORE = ["proj_hits", "proj_hr", "proj_avg", "proj_slg", "k_percent_eff", "bb_percent_eff"]
PITCHER_WOBA_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]
PITCHER_CTX_COLS = [
    "role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome",
    "game_id","home_team_id","game_id_ctx","team_id_ctx","park_factor_ctx","role_ctx",
    "city_ctx","state_ctx","timezone_ctx","is_dome_ctx"
]

def read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    # handle possible BOM, blank trailing columns
    return pd.read_csv(path, dtype=str).rename(columns=lambda c: c.strip())

def to_numeric(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def to_int(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").dropna().astype(int).reindex(df.index)
            # keep NaN where it was NaN
            nulls = df[c].isna()
            df.loc[nulls, c] = pd.NA

def sanitize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # lower_snake, strip weird spaces
    def norm(s: str) -> str:
        s = s.strip()
        s = s.replace("%","percent").replace("/","_").replace("+","plus").replace("-","_").replace(".","_")
        s = "_".join(s.split())
        return s.lower()
    return df.rename(columns={c: norm(c) for c in df.columns})

def ensure_unique(df: pd.DataFrame, key_cols: List[str], label: str):
    if not key_cols:
        return
    dupes = df.duplicated(subset=[c for c in key_cols if c in df.columns], keep=False)
    if dupes.any():
        rows = int(dupes.sum())
        raise ValueError(f"{label}: duplicate keys on {key_cols} for {rows} rows")

def bounds_check(df: pd.DataFrame, col: str, lo: float, hi: float, label: str):
    if col not in df.columns:
        return
    bad = df[(pd.to_numeric(df[col], errors="coerce") < lo) | (pd.to_numeric(df[col], errors="coerce") > hi)]
    if len(bad) > 0:
        raise ValueError(f"{label}: values out of bounds {lo}..{hi} in column '{col}' ({len(bad)} rows)")

def minmax(df: pd.DataFrame, cols: List[str]) -> Dict[str, Tuple[float,float]]:
    out = {}
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            out[c] = (float(s.min(skipna=True)), float(s.max(skipna=True)))
    return out

def copy_if_missing(left: pd.DataFrame, right: pd.DataFrame, keys: List[str], cols: List[str]) -> pd.DataFrame:
    # copy cols from right to left when left col is missing OR all NaN
    need_cols = [c for c in cols if c not in left.columns or left[c].isna().all()]
    if not need_cols:
        return left
    rcols = keys + [c for c in need_cols if c in right.columns]
    if len(rcols) <= len(keys):
        return left
    merged = left.merge(right[rcols].drop_duplicates(subset=[c for c in keys if c in right.columns]),
                        on=[c for c in keys if c in right.columns], how="left", suffixes=("","_r"))
    for c in need_cols:
        rc = c if c in right.columns else f"{c}_r"
        if rc in merged.columns:
            if c not in merged.columns:
                merged[c] = merged[rc]
            else:
                merged[c] = merged[c].fillna(merged[rc])
        if f"{c}_r" in merged.columns:
            merged.drop(columns=[f"{c}_r"], inplace=True, errors="ignore")
    return merged

def coerce_batter_types(df: pd.DataFrame) -> pd.DataFrame:
    # IDs
    for c in ["player_id","game_id"]:
        if c in df.columns:
            if c == "player_id":
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            else:
                df[c] = df[c].astype(str)
    # numerics
    num_cols = set(BATTER_PROB_COLS + BATTER_PROJ_CORE + BATTER_WOBA_COLS)
    for c in df.columns:
        if c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def coerce_pitcher_types(df: pd.DataFrame) -> pd.DataFrame:
    # IDs & context
    for c in ["player_id","game_id","team_id","home_team_id","opponent_team_id","role"]:
        if c in df.columns:
            if c == "player_id":
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            else:
                df[c] = df[c].astype(str)
    # numerics
    num_cols = set(PITCHER_PROJ_CORE + PITCHER_WOBA_COLS + ["park_factor","park_factor_ctx",
                       "k_percent","bb_percent","k_percent_eff","bb_percent_eff"])
    for c in df.columns:
        if c in num_cols or c.endswith("_percent") or c.startswith("prob_") or c.startswith("proj_"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def write_both(df: pd.DataFrame, key: str, summary: List[str]):
    OUT_DIR_A.mkdir(parents=True, exist_ok=True)
    OUT_DIR_B.mkdir(parents=True, exist_ok=True)
    a = OUT_DIR_A / OUTFILES[key]
    b = OUT_DIR_B / OUTFILES[key]
    df.to_csv(a, index=False)
    df.to_csv(b, index=False)
    summary.append(f"✅ Wrote {key}: {len(df):,} rows -> {a} AND {b}")

def main() -> int:
    print("▶️ START: finalize_projections.py")
    summary = []

    # 1) Load
    dfs = {}
    for k, p in INPUTS.items():
        df = read_csv_safe(p)
        if df.empty:
            if k == "pitcher_mega_z":
                summary.append("⚠️ pitcher_mega_z_fixed.csv not found or empty — skipping that file.")
            else:
                print(f"❌ Missing or empty required file: {p}")
                return 2
        dfs[k] = sanitize_headers(df)

    # 2) Batters — Expanded
    be = dfs["batters_expanded"].copy()
    if be.empty:
        print("❌ batters_expanded is empty")
        return 2
    be = coerce_batter_types(be)
    # Basic required columns
    need_be = ["player_id"] + BATTER_PROJ_CORE + BATTER_PROB_COLS
    missing = [c for c in need_be if c not in be.columns]
    if missing:
        print(f"❌ batter_props_expanded_fixed missing columns: {missing}")
        return 2
    ensure_unique(be, [c for c in ["player_id","game_id"] if c in be.columns], "batters_expanded")
    for c in BATTER_PROB_COLS:
        bounds_check(be, c, 0.0, 1.0, "batters_expanded")

    # 3) Batters — Projected
    bp = dfs["batters_projected"].copy()
    if bp.empty:
        print("❌ batters_projected is empty")
        return 2
    bp = coerce_batter_types(bp)
    need_bp = ["player_id"] + BATTER_PROJ_CORE + BATTER_PROB_COLS
    missing = [c for c in need_bp if c not in bp.columns]
    if missing:
        print(f"❌ batter_props_projected_fixed missing columns: {missing}")
        return 2
    ensure_unique(bp, [c for c in ["player_id","game_id"] if c in bp.columns], "batters_projected")
    for c in BATTER_PROB_COLS:
        bounds_check(bp, c, 0.0, 1.0, "batters_projected")

    # 4) Copy adj_woba_* across batter files if absent in one but present in the other
    be = copy_if_missing(be, bp, ["player_id","game_id"] if "game_id" in be.columns and "game_id" in bp.columns else ["player_id"], BATTER_WOBA_COLS)
    bp = copy_if_missing(bp, be, ["player_id","game_id"] if "game_id" in be.columns and "game_id" in bp.columns else ["player_id"], BATTER_WOBA_COLS)

    # 5) Pitchers — Projected
    pp = dfs["pitchers_projected"].copy()
    if pp.empty:
        print("❌ pitchers_projected is empty")
        return 2
    pp = coerce_pitcher_types(pp)
    need_pp = ["player_id"] + PITCHER_PROJ_CORE + PITCHER_WOBA_COLS
    missing = [c for c in need_pp if c not in pp.columns]
    if missing:
        print(f"❌ pitcher_props_projected_fixed missing columns: {missing}")
        return 2
    ensure_unique(pp, [c for c in ["player_id","game_id","role"] if c in pp.columns], "pitchers_projected")

    # light context keep: only keep context cols if present
    keep_cols_pp = list(dict.fromkeys(
        PITCHER_KEY + PITCHER_WOBA_COLS + PITCHER_PROJ_CORE + [c for c in PITCHER_CTX_COLS if c in pp.columns] + [c for c in pp.columns if c in ("ab","innings_pitched","pa")]
    ))
    pp = pp[keep_cols_pp]

    # 6) Pitcher Mega Z — optional, pass-through with header clean
    pmz = dfs.get("pitcher_mega_z", pd.DataFrame()).copy()
    if not pmz.empty:
        pmz = sanitize_headers(pmz)

    # 7) Write outputs
    write_both(be, "batters_expanded", summary)
    write_both(bp, "batters_projected", summary)
    write_both(pp, "pitchers_projected", summary)
    if not pmz.empty:
        write_both(pmz, "pitcher_mega_z", summary)

    # 8) Summary
    def fmt_minmax(d: Dict[str, Tuple[float,float]]) -> str:
        return ", ".join([f"{k}[{v[0]:.3f}..{v[1]:.3f}]" for k,v in d.items()])

    be_mm = fmt_minmax(minmax(be, BATTER_PROB_COLS))
    bp_mm = fmt_minmax(minmax(bp, BATTER_PROB_COLS))
    pp_mm = fmt_minmax(minmax(pp, ["k_percent_eff","bb_percent_eff","proj_avg","proj_slg"]))

    print("===== SUMMARY =====")
    print(f"batters_expanded rows: {len(be):,} | probs {be_mm}")
    print(f"batters_projected rows: {len(bp):,} | probs {bp_mm}")
    print(f"pitchers_projected rows: {len(pp):,} | k%/bb%/avg/slg {pp_mm}")
    if not pmz.empty:
        print(f"pitcher_mega_z rows: {len(pmz):,}")
    for line in summary:
        print(line)
    print("✅ finalize_projections.py completed")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
