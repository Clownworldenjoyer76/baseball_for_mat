#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
append_player_history_from_prep.py  (REPLACEMENT)

New behavior (per your spec):
- Treats this as a **daily overwrite**, not an append.
- Reads a "prep" CSV of player props.
- Filters to **today's date** (fallback: if today's not present, uses the most recent date in the file).
- Keeps **max 5 props per player** (by highest `over_probability`).
- Adds/updates a `timestamp` column (ISO-8601, local time).
- Sorts each player's rows by highest `over_probability` and assigns `prop_rank` (1 = highest).
- Overwrites the output CSV instead of appending (no history kept here).

Usage:
  python scripts/append_player_history_from_prep.py \
      --input data/prep/player_props_prep.csv \
      --output data/player_props_history.csv

If --input is omitted, the script will try to auto-detect a prep CSV in:
  - data/prep/*.csv
  - data/*.csv
"""

import argparse
import sys
import os
import glob
import pandas as pd
from datetime import datetime, date
from typing import Optional

def _find_default_prep_csv() -> Optional[str]:
    """Try to locate a reasonable default prep CSV if none provided."""
    search_paths = [
        "data/prep/*.csv",
        "data/*.csv",
    ]
    candidates = []
    for pattern in search_paths:
        candidates.extend(glob.glob(pattern))
    if not candidates:
        return None
    # pick the most recently modified file
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Overwrite daily player props with top 5 per player for today's games.")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to the prep CSV (if omitted, the script attempts to auto-detect).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/player_props_history.csv",
        help="Path to write the overwritten CSV (default: data/player_props_history.csv).",
    )
    return parser.parse_args()

def _ensure_columns(df: pd.DataFrame, needed: list):
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(
            f"Prep file is missing required column(s): {missing}. "
            f"At minimum, the prep CSV must include these columns: {needed}."
        )

def _normalize_date_str(x) -> Optional[str]:
    # Expecting 'YYYY-MM-DD' in prep. If not, try to parse.
    if pd.isna(x):
        return None
    try:
        s = str(x).strip()
        # If already YYYY-MM-DD, return as-is
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return s
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def main():
    args = _parse_args()

    prep_path = args.input or _find_default_prep_csv()
    if not prep_path or not os.path.exists(prep_path):
        print(
            "ERROR: Could not locate a prep CSV. "
            "Provide --input or place a prep file in data/prep/ or data/.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        df = pd.read_csv(prep_path)
    except Exception as e:
        print(f"ERROR: Failed to read prep CSV at {prep_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Required columns we rely on
    required_cols = [
        "date",              # game date
        "player_id",         # stable player identifier (used to group)
        "name",              # display name
        "team",              # team name/abbr
        "prop",              # prop type (e.g., hits, total_bases, hr, etc.)
        "over_probability",  # probability we want to sort/limit by
    ]
    _ensure_columns(df, required_cols)

    # Normalize date to YYYY-MM-DD strings
    df["date"] = df["date"].map(_normalize_date_str)

    if df["date"].isna().all():
        print("ERROR: Could not parse any valid dates from the 'date' column in the prep CSV.", file=sys.stderr)
        sys.exit(1)

    # Determine "today" and filter
    today_str = date.today().strftime("%Y-%m-%d")
    df_today = df[df["date"] == today_str].copy()

    if df_today.empty:
        # Fallback to most recent date available in the prep CSV (so the job still produces output)
        # This avoids a silent empty file if timezones differ or the prep was generated for tomorrow.
        # If you strictly want no output when today's not present, remove this fallback block.
        most_recent = (
            df["date"].dropna().sort_values(ascending=True).iloc[-1]
        )
        df_today = df[df["date"] == most_recent].copy()
        print(
            f"WARNING: No rows matched today's date ({today_str}). "
            f"Using most recent date found in prep: {most_recent}.",
            file=sys.stderr,
        )

    # Add/refresh timestamp (local ISO-8601)
    now_iso = datetime.now().astimezone().isoformat()
    df_today["timestamp"] = now_iso

    # Sort by over_probability (desc) within each player, keep top 5 props
    # If probabilities are missing, treat as very low so they drop to bottom.
    df_today["over_probability"] = pd.to_numeric(df_today["over_probability"], errors="coerce")
    df_today["over_probability"] = df_today["over_probability"].fillna(-1.0)

    # Rank props within (player_id, date) by over_probability desc
    df_today = df_today.sort_values(["player_id", "date", "over_probability"], ascending=[True, True, False])
    df_today["prop_rank"] = df_today.groupby(["player_id", "date"])["over_probability"].rank(
        method="first", ascending=False
    )

    # Keep top 5 per player for that date
    df_today = df_today[df_today["prop_rank"] <= 5].copy()

    # Optional: Keep a consistent output column order if present in prep
    preferred_order = [
        "name",
        "player_id",
        "team",
        "date",
        "prop",
        "over_probability",
        "prop_rank",
        "timestamp",
    ]
    # plus any extras the prep may have included (e.g., lines, z-scores, opponents, value, etc.)
    remaining = [c for c in df_today.columns if c not in preferred_order]
    out_cols = [c for c in preferred_order if c in df_today.columns] + remaining
    df_today = df_today[out_cols]

    # Make sure output directory exists
    out_path = args.output
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # OVERWRITE (not append)
    try:
        df_today.to_csv(out_path, index=False)
    except Exception as e:
        print(f"ERROR: Failed to write output CSV at {out_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Simple stdout summary
    players = df_today["player_id"].nunique()
    rows = len(df_today)
    msg_date = df_today["date"].iloc[0] if not df_today.empty else today_str
    print(
        f"✅ Wrote {rows} rows for {players} players "
        f"(max 5 props per player) for date={msg_date} → {out_path}"
    )

if __name__ == "__main__":
    main()
