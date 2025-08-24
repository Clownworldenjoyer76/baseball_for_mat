#!/usr/bin/env python3
"""
Append/curate batter props from a prep CSV.

Behavior:
- Loads a "prep" CSV (default: data/batter_props_final.csv).
- If the 'date' column is missing, it is auto-filled with today's date (YYYY-MM-DD).
- Filters to rows for *today* only.
- For each prop (e.g., 'hits', 'total_bases', 'hr', etc.), keeps the top 5 rows
  by 'over_probability' (highest first).
- Adds 'timestamp' (local timezone ISO8601) for traceability.
- Adds 'prop_sort' = rank within each prop by 'over_probability' (1 = highest).
- Overwrites the output CSV (default: same path as input, so it rewrites in place).

CLI:
  python scripts/append_player_history_from_prep.py \
      --prep-csv data/batter_props_final.csv \
      --out-csv  data/batter_props_final.csv
"""
from __future__ import annotations

import argparse
import sys
import os
from datetime import datetime, date, timezone
import pandas as pd

# ---------- Utils ----------

TODAY_STR = date.today().isoformat()

REQUIRED_MIN = ["player_id", "name", "team", "prop", "over_probability"]
OPTIONAL_NUMERIC = ["line", "value", "batter_z", "mega_z",
                    "opp_pitcher_z", "opp_pitcher_mega_z"]
OPTIONAL_TEXT = ["opp_team", "opp_pitcher_name"]

def _read_csv_any(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Prep CSV not found at '{path}'.")
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise RuntimeError(f"Failed reading '{path}': {e}")

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # If 'date' missing, create and fill with today.
    if "date" not in df.columns:
        df["date"] = TODAY_STR

    # Normalize column types (best-effort)
    for col in REQUIRED_MIN:
        if col not in df.columns:
            raise ValueError(
                "Prep CSV is missing required column(s). "
                f"At minimum it must include: {REQUIRED_MIN + ['date']}. "
                f"Found: {list(df.columns)}"
            )

    # Coerce types safely
    # over_probability should be numeric in [0,1]
    df["over_probability"] = pd.to_numeric(df["over_probability"], errors="coerce")
    # prop/name/team textual
    for c in ["prop", "name", "team"]:
        df[c] = df[c].astype(str)

    # Optional numeric/text
    for c in OPTIONAL_NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in OPTIONAL_TEXT:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # Normalize date to YYYY-MM-DD strings
    # (accepts datetime-like or strings)
    def _to_date_str(x):
        try:
            return pd.to_datetime(x).date().isoformat()
        except Exception:
            return TODAY_STR  # fallback; keeps us unblocked
    df["date"] = df["date"].apply(_to_date_str)

    return df

def _filter_today(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["date"] == TODAY_STR].copy()

def _top5_per_prop(df: pd.DataFrame) -> pd.DataFrame:
    # Sort by over_probability desc, then take 5 per prop
    df_sorted = df.sort_values(["prop", "over_probability"], ascending=[True, False])
    return df_sorted.groupby("prop", group_keys=False).head(5)

def _add_timestamp_and_rank(df: pd.DataFrame) -> pd.DataFrame:
    # Local timestamp ISO8601 (with offset)
    local_iso = datetime.now().astimezone().isoformat()
    df["timestamp"] = local_iso

    # prop_sort: rank within each prop by over_probability (1 = highest)
    df["prop_sort"] = (
        df.groupby("prop")["over_probability"]
          .rank(method="first", ascending=False)
          .astype(int)
    )

    return df

def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Put the most useful columns first; keep others afterward in stable order.
    preferred = [
        "date", "timestamp",
        "player_id", "name", "team",
        "prop", "line", "value",
        "over_probability",
        "prop_sort",
        "batter_z", "mega_z",
        "opp_team", "opp_pitcher_name", "opp_pitcher_z", "opp_pitcher_mega_z",
    ]
    existing_pref = [c for c in preferred if c in df.columns]
    rest = [c for c in df.columns if c not in existing_pref]
    return df[existing_pref + rest]

# ---------- Main ----------

def run(prep_csv: str, out_csv: str) -> None:
    df = _read_csv_any(prep_csv)
    df = _ensure_columns(df)

    # Today only
    df = _filter_today(df)

    # If nothing for today, write an empty file (with headers) to avoid stale data.
    if df.empty:
        # Build an empty frame with the required headers plus helpers.
        cols = list(dict.fromkeys(
            ["date", "timestamp"] + REQUIRED_MIN + OPTIONAL_NUMERIC + OPTIONAL_TEXT + ["prop_sort"]
        ))
        empty = pd.DataFrame(columns=cols)
        _write_out(empty, out_csv)
        print(f"[OK] No rows for today ({TODAY_STR}). Wrote empty CSV to {out_csv}.")
        return

    # Keep top 5 per prop
    df = _top5_per_prop(df)

    # Add timestamp and ranking
    df = _add_timestamp_and_rank(df)

    # Reorder columns for readability
    df = _reorder_columns(df)

    # Overwrite output file
    _write_out(df, out_csv)
    print(f"[OK] Wrote {len(df)} rows (top 5 per prop for {TODAY_STR}) to {out_csv}.")

def _write_out(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Overwrite (no append)
    df.to_csv(path, index=False)

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Curate today's batter props and overwrite output.")
    p.add_argument("--prep-csv",
                   default="data/batter_props_final.csv",
                   help="Path to the prep CSV to read (default: data/batter_props_final.csv).")
    p.add_argument("--out-csv",
                   default=None,
                   help="Path to write. Defaults to the same as --prep-csv (overwrite).")
    return p.parse_args(argv)

def main() -> None:
    args = parse_args(sys.argv[1:])
    out_csv = args.out_csv or args.prep_csv  # overwrite by default
    run(prep_csv=args.prep_csv, out_csv=out_csv)

if __name__ == "__main__":
    main()
