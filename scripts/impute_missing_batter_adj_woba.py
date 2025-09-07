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

Imputation Formulae:
  Let i index a batter row with missing values and fields W=adj_woba_weather, P=adj_woba_park.

  Sets:
    G(i)  := rows with same game_id as row i.
    GT(i) := rows with same game_id and same team as row i.

  Statistics (excluding missing/empty):
    med_G(W), med_G(P), med_GT(W), med_GT(P), med_ALL(W), med_ALL(P)

  Fill order:
    W_i := first_non_null(med_GT(W), med_G(W), med_ALL(W))
    P_i := first_non_null(med_GT(P), med_G(P), med_ALL(P))
    adj_woba_combined_i := (W_i + P_i) / 2

Logging:
  A CSV of all imputed rows is written beside each file with suffix:
    *_imputed_adj_woba_rows.csv
"""

import os
import sys
import shutil
import pandas as pd
from typing import Tuple, List, Dict, Any

# Default inputs (can be overridden via CLI)
INPUT_FILES = [
    "data/_projections/batter_props_expanded_final.csv",
    "data/_projections/batter_props_projected_final.csv",
]

ADJ_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]
ID_COLS_PREF = ["player_id", "name", "team", "game_id"]


def _is_missing_series(s: pd.Series) -> pd.Series:
    return s.isna() | (s.astype(str).str.strip() == "")


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> None:
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
    _coerce_numeric(df, ADJ_COLS)
    for c in ADJ_COLS:
        df.loc[_is_missing_series(df[c]), c] = pd.NA
    return df


def _compute_global_medians(df: pd.DataFrame) -> Tuple[float, float]:
    mw = _median_or_none(df["adj_woba_weather"])
    mp = _median_or_none(df["adj_woba_park"])
    return mw, mp


def _id_cols_present(df: pd.DataFrame) -> List[str]:
    return [c for c in ID_COLS_PREF if c in df.columns]


def _impute_row(df: pd.DataFrame, idx: int,
                med_all_w: float, med_all_p: float) -> Tuple[bool, Dict[str, Any]]:
    row = df.loc[idx]
    changed = False
    before = row.copy()

    has_team = "team" in df.columns
    has_game = "game_id" in df.columns

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

    # Fill weather
    if pd.isna(row["adj_woba_weather"]):
        for cand in (med_gt_w, med_g_w, med_all_w):
            if cand is not None:
                df.at[idx, "adj_woba_weather"] = cand
                changed = True
                break

    # Fill park
    if pd.isna(df.at[idx, "adj_woba_park"]):
        for cand in (med_gt_p, med_g_p, med_all_p):
            if cand is not None:
                df.at[idx, "adj_woba_park"] = cand
                changed = True
                break

    # Combined
    if pd.isna(df.at[idx, "adj_woba_combined"]) or changed:
        w = df.at[idx, "adj_woba_weather"]
        p = df.at[idx, "adj_woba_park"]
        if pd.notna(w) and pd.notna(p):
            df.at[idx, "adj_woba_combined"] = (float(w) + float(p)) / 2.0
            changed = True

    after = df.loc[idx]
    delta = {}
    for c in ADJ_COLS:
        b = None if pd.isna(before.get(c)) else float(before.get(c))
        a = None if pd.isna(after.get(c)) else float(after.get(c))
        if b != a:
            delta[c] = {"from": b, "to": a}

    return changed, delta


def process_file(path: str) -> None:
    if not os.path.exists(path):
        print(f"[SKIP] Missing file: {path}")
        return

    df = pd.read_csv(path)
    df = _prepare(df)

    miss_mask = (
        _is_missing_series(df["adj_woba_weather"]) |
        _is_missing_series(df["adj_woba_park"]) |
        _is_missing_series(df["adj_woba_combined"])
    )

    if not miss_mask.any():
        print(f"[OK] No missing adj_woba_* in: {path}")
        return

    med_all_w, med_all_p = _compute_global_medians(df)

    changed_rows: List[Dict[str, Any]] = []
    for idx in df.index[miss_mask].tolist():
        changed, delta = _impute_row(df, idx, med_all_w, med_all_p)
        if changed:
            meta = {c: df.at[idx, c] for c in _id_cols_present(df)}
            meta.update({k: v for k, v in delta.items()})
            changed_rows.append(meta)

    backup_path = path + ".bak"
    shutil.copyfile(path, backup_path)
    df.to_csv(path, index=False)

    log_dir = os.path.dirname(path)
    base = os.path.splitext(os.path.basename(path))[0]
    log_path = os.path.join(log_dir, f"{base}_imputed_adj_woba_rows.csv")
    pd.DataFrame(changed_rows).to_csv(log_path, index=False)

    print(f"[UPDATED] {path}")
    print(f"[BACKUP ] {backup_path}")
    print(f"[LOG    ] {log_path}")


def main(cli_args: List[str]) -> int:
    files = INPUT_FILES
    if len(cli_args) == 2:
        files = [cli_args[0], cli_args[1]]
    elif len(cli_args) not in (0, 2):
        print("Usage:")
        print("  python /scripts/impute_missing_batter_adj_woba.py")
        print("  python /scripts/impute_missing_batter_adj_woba.py <expanded_csv> <projected_csv>")
        return 2

    for p in files:
        process_file(p)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
