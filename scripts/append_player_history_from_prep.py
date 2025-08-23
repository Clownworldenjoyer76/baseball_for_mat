#!/usr/bin/env python3
# scripts/append_player_history_from_prep.py
"""
Rewrite today's player prop rows from the prep CSV into the player_props_history.csv.

- Reads prep CSV (default: data/bets/prep/batter_props_final.csv)
- Filters to today's date (YYYY-MM-DD in the CSV's `date` column)
- For each player_id, keeps at most the top 5 props by `over_probability` (descending)
- Normalizes columns and values
- Overwrites the output CSV (does NOT append)

Usage:
  python scripts/append_player_history_from_prep.py \
      --prep-csv data/bets/prep/batter_props_final.csv \
      --out-csv  data/bets/player_props_history.csv \
      [--date 2025-08-23]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

REQUIRED_COLS: List[str] = [
    "date", "player_id", "name", "team",
    "prop", "line", "value", "over_probability",
]

def _fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr); sys.exit(1)

def _ensure_columns(df: pd.DataFrame, required: List[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        preview = ", ".join(df.columns.tolist())
        _fail(
            "Prep CSV is missing required column(s): "
            f"{missing}. Columns found: [{preview}]."
        )

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if "prop" in df.columns:
        df["prop"] = df["prop"].astype(str).str.strip().str.lower()
    if "team" in df.columns:
        df["team"] = df["team"].astype(str).str.strip()
    if "name" in df.columns:
        df["name"] = df["name"].astype(str).str.strip()
    for c in ("line", "value", "over_probability"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["player_id", "prop", "over_probability"])

def _pick_top5_per_player(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(
        ["player_id", "over_probability", "value"],
        ascending=[True, False, False]
    ).copy()
    df["prop_sort"] = df.groupby("player_id")["over_probability"].rank(
        method="first", ascending=False
    )
    keep_mask = df.groupby("player_id").cumcount() < 5
    return df.loc[keep_mask].reset_index(drop=True)

def run(prep_csv: str, out_csv: str, date_str: str | None) -> None:
    prep_path = Path(prep_csv)
    if not prep_path.exists():
        _fail(f"Prep CSV not found at '{prep_csv}'. Provide the correct path via --prep-csv.")

    try:
        df = pd.read_csv(prep_path)
    except Exception as e:
        _fail(f"Failed to read prep CSV '{prep_csv}': {e}")

    _ensure_columns(df, REQUIRED_COLS)
    df = _normalize(df)

    if date_str:
        keep_date = date_str
    else:
        today = datetime.now().date().isoformat()
        keep_date = today if today in set(df["date"].astype(str).unique()) \
                    else str(pd.to_datetime(df["date"]).dt.date.max())

    df = df[df["date"].astype(str) == keep_date].copy()
    if df.empty:
        _fail(f"No rows for date {keep_date} in '{prep_csv}'.")

    df = _pick_top5_per_player(df)

    ordered_cols = [
        "player_id", "name", "team", "prop", "line", "value",
        "over_probability", "date",
    ]
    extra = [c for c in df.columns if c not in ordered_cols + ["prop_sort"]]
    df_out = df[ordered_cols + ["prop_sort"] + extra]

    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False)
    print(
        f"Wrote {len(df_out)} rows for {keep_date} to '{out_csv}' "
        f"(covering {df_out['player_id'].nunique()} players, max 5 props each)."
    )

def main() -> None:
    p = argparse.ArgumentParser(
        description="Overwrite player_props_history.csv with today's top 5 props/player."
    )
    p.add_argument("--prep-csv", default="data/bets/prep/batter_props_final.csv")
    p.add_argument("--out-csv",  default="data/bets/player_props_history.csv")
    p.add_argument("--date", help="YYYY-MM-DD (optional)")
    args = p.parse_args()
    run(args.prep_csv, args.out_csv, args.date)

if __name__ == "__main__":
    main()
