#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Purpose:
  Deterministically fill missing adj_woba_* fields in batter projection files.

Files:
  Input:
    - data/_projections/batter_props_expanded_final.csv
    - data/_projections/batter_props_projected_final.csv
  Output (in-place overwrite + backups):
    - data/_projections/batter_props_expanded_final.csv
    - data/_projections/batter_props_projected_final.csv
  Backups:
    - data/_projections/batter_props_expanded_final.csv.bak
    - data/_projections/batter_props_projected_final.csv.bak

Imputation Formulae (no assumptions beyond dataset statistics):
  Let i index a batter row with missing values and fields W=adj_woba_weather, P=adj_woba_park.

  Sets:
    G(i)  := all rows with the same game_id as row i.
    GT(i) := all rows with the same game_id and same team as row i.

  Statistics (computed excluding missing/empty values):
    med_G(W)   := median of W over G(i)
    med_G(P)   := median of P over G(i)
    med_GT(W)  := median of W over GT(i)
    med_GT(P)  := median of P over GT(i)
    med_ALL(W) := median of W over the entire file
    med_ALL(P) := median of P over the entire file

  Fill order for each missing field:
    If W_i is missing:
        W_i := first available in [ med_GT(W), med_G(W), med_ALL(W) ]
    If P_i is missing:
        P_i := first available in [ med_GT(P), med_G(P), med_ALL(P) ]

  Combined adjustment:
    adj_woba_combined_i := (adj_woba_weather_i + adj_woba_park_i) / 2.0

  Notes:
    - “missing” means NaN or empty string after strip.
    - Median is used for robustness to outliers.
    - If a chosen set has no valid values, fall through to the next set.

Logging:
  A CSV of all imputed rows is written beside each file with suffix:
    *_imputed_adj_woba_rows.csv
"""

import os
import sys
import shutil
import pandas as pd
from typing import Tuple

INPUT_FILES = [
    "data/_projections/batter_props_expanded_final.csv",
    "data/_projections/batter_props_projected_final.csv",
]

ADJ_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

ID_COLS_PREF = ["player_id", "name", "team", "game_id"]
GROUP_KEYS_GAME = ["game_id"]
GROUP_KEYS_GAMETEAM = ["game_id", "team"]


def _is_missing_series(s: pd.Series) -> pd.Series:
    return s.isna() | (s.astype(str).str.strip() == "")


def _coerce_numeric(df: pd.DataFrame, cols) -> None:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = pd.to_numeric(df[c], errors="coerce")


def _median_or_none(series: pd.Series):
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.median())


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure required columns exist and numeric
    _coerce_numeric(df, ADJ_COLS)
    # Standardize missing
    for c in ADJ_COLS:
        df.loc[_is_missing_series(df[c]), c] = pd.NA
    return df


def _compute_global_medians(df: pd.DataFrame) -> Tuple[float, float]:
    mw = _median_or_none(df["adj_woba_weather"])
    mp = _median_or_none(df["adj_woba_park"])
    return mw, mp


def _impute_row(df: pd.DataFrame, idx: int,
                med_all_w: float, med_all_p: float) -> Tuple[bool, dict]:
    row = df.loc[idx]
    changed = False
    before = row.copy()

    # Compute medians for the groups relevant to this row
    # Use safe filters in case columns are missing
    has_team = "team" in df.columns
    has_game = "game_id" in df.columns

    # Defaults
    med_gt_w = med_g_w = None
    med_gt_p = med_g_p = None

    if has_game:
        gmask = df["game_id"] == row["game_id"]
        gdf = df.loc[gmask]
        med_g_w = _median_or_none(gdf["adj_woba_weather"])
        med_g_p = _median_or_none(gdf["adj_woba_park"])

        if has_team:
            gtmask = gmask & (df["team"] == row.get("team"))
            gtdf = df.loc[gtmask]
            med_gt_w = _median_or_none(gtdf["adj_woba_weather"])
            med_gt_p = _median_or_none(gtdf["adj_woba_park"])

    # Fill adj_woba_weather if missing
    if pd.isna(row["adj_woba_weather"]):
        for cand in (med_gt_w, med_g_w, med_all_w):
            if cand is not None:
                df.at[idx, "adj_woba_weather"] = cand
                changed = True
                break

    # Fill adj_woba_park if missing
    if pd.isna(df.at[idx, "adj_woba_park"]):
        for cand in (med_gt_p, med_g_p, med_all_p):
            if cand is not None:
                df.at[idx, "adj_woba_park"] = cand
                changed = True
                break

    # Recompute combined if either component is missing or if combined missing
    if pd.isna(df.at[idx, "adj_woba_combined"]) or changed:
        w = df.at[idx, "adj_woba_weather"]
        p = df.at[idx, "adj_woba_park"]
        if pd.notna(w) and pd.notna(p):
            df.at[idx, "adj_woba_combined"] = (float(w) + float(p)) / 2.0
            changed = True

    after = df.loc[idx]
    delta = {}
    for c in ADJ_COLS:
        if before.get(c) is pd.NA and after.get(c) is not pd.NA:
            delta[c] = {"from": None, "to": float(after.get(c))}
        elif pd.isna(before.get(c)) and pd.notna(after.get(c)):
            delta[c] = {"from": None, "to": float(after.get(c))}
        elif before.get(c) != after.get(c):
            # Covers case where combined recalculated
            b = None if pd.isna(before.get(c)) else float(before.get(c))
            a = None if pd.isna(after.get(c)) else float(after.get(c))
            delta[c] = {"from": b, "to": a}

    return changed, delta


def _id_cols_present(df: pd.DataFrame):
    return [c for c in ID_COLS_PREF if c in df.columns]


def process_file(path: str) -> None:
    if not os.path.exists(path):
        print(f"[SKIP] Missing file: {path}")
        return

    df = pd.read_csv(path)
    df = _prepare(df)

    # Identify rows needing imputation
    miss_mask = (
        _is_missing_series(df["adj_woba_weather"]) |
        _is_missing_series(df["adj_woba_park"]) |
        _is_missing_series(df["adj_woba_combined"])
    )

    if not miss_mask.any():
        print(f"[OK] No missing adj_woba_* in: {path}")
        return

    # Compute global medians once
    med_all_w, med_all_p = _compute_global_medians(df)

    changed_rows = []
    for idx in df.index[miss_mask].tolist():
        changed, delta = _impute_row(df, idx, med_all_w, med_all_p)
        if changed:
            meta = {c: df.at[idx, c] for c in _id_cols_present(df)}
            meta.update({k: v for k, v in delta.items()})
            changed_rows.append(meta)

    # Write backup, then overwrite
    backup_path = path + ".bak"
    shutil.copyfile(path, backup_path)
    df.to_csv(path, index=False)

    # Emit an imputation log for this file
    log_dir = os.path.dirname(path)
    base = os.path.splitext(os.path.basename(path))[0]
    log_path = os.path.join(log_dir, f"{base}_imputed_adj_woba_rows.csv")
    pd.DataFrame(changed_rows).to_csv(log_path, index=False)

    print(f"[UPDATED] {path}")
    print(f"[BACKUP ] {backup_path}")
    print(f"[LOG    ] {log_path}")


def main():
    for p in INPUT_FILES:
        process_file(p)


if __name__ == "__main__":
    # Allow optional override paths via CLI args (two inputs expected)
    # Usage:
    #   python /scripts/impute_missing_batter_adj_woba.py
    #   python /scripts/impute_missing_batter_adj_woba.py <expanded_csv> <projected_csv>
    global INPUT_FILES
    if len(sys.argv) == 3:
        INPUT_FILES = [sys.argv[1], sys.argv[2]]
    elif len(sys.argv) != 1:
        print("Usage:")
        print("  python /scripts/impute_missing_batter_adj_woba.py")
        print("  python /scripts/impute_missing_batter_adj_woba.py <expanded_csv> <projected_csv>")
        sys.exit(2)
    main()
