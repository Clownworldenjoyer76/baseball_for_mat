#!/usr/bin/env python3

"""
Replace existing history-appender with a strict, deterministic writer.

What it does (per your requirements):
- Reads today's props from a single "prep" CSV.
- Normalizes columns (prop lower/stripped, names trimmed, etc.).
- Filters to ONLY today's games.
- Keeps at most 5 props per player, ranked by over_probability (highest first).
- Writes a single CSV for today, **overwriting** the prior file (no appends).
- Adds an ISO timestamp column.
- Provides a prop_sort rank where 1 = highest probability for that player.

Default I/O (override via CLI):
  --prep-csv  data/props_prep.csv
  --out-csv   data/final_player_props.csv
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, date
from typing import Dict, List

import pandas as pd


# --- config --------------------------------------------------------------

REQUIRED_COLS = [
    "date",              # string or date; expected format YYYY-MM-DD (flexible parsing)
    "player_id",         # int/str
    "name",              # str
    "team",              # str (team name/abbr; we just carry it through)
    "prop",              # str (e.g., "hits", "total_bases")
    "over_probability",  # float in [0,1]
]

# Permissive alternative names we’ll map into REQUIRED_COLS if present.
ALT_COL_MAP: Dict[str, str] = {
    "player": "name",
    "player_name": "name",
    "playerid": "player_id",
    "player_id_mlb": "player_id",
    "mlb_id": "player_id",
    "team_name": "team",
    "team_abbr": "team",
    "prop_type": "prop",
    "prob_over": "over_probability",
    "over_prob": "over_probability",
    "probability_over": "over_probability",
}

TOP_N_PER_PLAYER = 5


# --- helpers -------------------------------------------------------------

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Trim column headers
    df.columns = [c.strip() for c in df.columns]

    # Map alternates -> required
    for src, dst in ALT_COL_MAP.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]

    # Ensure required columns exist
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Prep file is missing required column(s): {missing}. "
            f"At minimum, the prep CSV must include these columns: {REQUIRED_COLS}."
        )

    # Normalize types / text
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if df["date"].isna().any():
        bad = df[df["date"].isna()]
        raise ValueError(
            f"Could not parse some 'date' values to dates. Example rows:\n{bad.head(5)}"
        )

    df["player_id"] = df["player_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["team"] = df["team"].astype(str).str.strip()

    # prop as canonical lowercase token (e.g., "hits", "total_bases")
    df["prop"] = df["prop"].astype(str).str.strip().str.lower()

    # over_probability to float bounded [0,1]
    df["over_probability"] = pd.to_numeric(df["over_probability"], errors="coerce")
    if df["over_probability"].isna().any():
        bad = df[df["over_probability"].isna()]
        raise ValueError(
            "Some 'over_probability' values are not numeric. Example rows:\n"
            f"{bad[['player_id','name','prop','over_probability']].head(5)}"
        )
    df["over_probability"] = df["over_probability"].clip(lower=0.0, upper=1.0)

    return df


def _today_local_date() -> date:
    # Use local system date; if your runner is UTC and you want US/Eastern,
    # change to: datetime.now(tz=ZoneInfo('America/New_York')).date()
    return datetime.now().date()


def _filter_to_today(df: pd.DataFrame) -> pd.DataFrame:
    today = _today_local_date()
    return df[df["date"] == today].copy()


def _dedupe_and_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Drop perfect duplicate rows across key fields.
    - Within each player_id for the day, sort props by over_probability desc.
    - Keep TOP_N_PER_PLAYER.
    - Create prop_sort rank (1 = highest).
    """
    if df.empty:
        return df

    key_cols = ["date", "player_id", "prop", "team", "name", "over_probability"]
    df = df.drop_duplicates(subset=key_cols, keep="first")

    df = df.sort_values(
        by=["player_id", "over_probability", "prop"],
        ascending=[True, False, True],
        kind="mergesort",
    )

    # Rank within player_id by probability (1 is best)
    df["prop_sort"] = (
        df.groupby(["player_id"])["over_probability"]
          .rank(method="first", ascending=False).astype(int)
    )

    # Keep only top N per player
    df = df[df["prop_sort"] <= TOP_N_PER_PLAYER].copy()

    return df


def _attach_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    # ISO 8601 with local offsetless time (if you want timezone, add %z and set tz-aware now)
    ts = datetime.now().isoformat(timespec="seconds")
    df["timestamp"] = ts
    return df


# --- main ---------------------------------------------------------------

def run(prep_csv: str, out_csv: str) -> None:
    # Read
    try:
        df = pd.read_csv(prep_csv)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Prep CSV not found at '{prep_csv}'. Provide the correct path via --prep-csv."
        )

    # Normalize / validate
    df = _standardize_columns(df)

    # Only today's games
    df = _filter_to_today(df)

    # If nothing for today, still write an empty file with headers (deterministic CI)
    if df.empty:
        empty = pd.DataFrame(columns=REQUIRED_COLS + ["prop_sort", "timestamp"])
        empty.to_csv(out_csv, index=False)
        print(f"[append_player_history_from_prep] No rows for today. Wrote empty file to {out_csv}")
        return

    # Rank, cap at 5 props per player, attach timestamp
    df = _dedupe_and_rank(df)
    df = _attach_timestamp(df)

    # Final column order (stable)
    final_cols: List[str] = [
        "date", "player_id", "name", "team", "prop", "over_probability",
        "prop_sort", "timestamp",
    ]
    # keep any extra columns at the end (don’t break if upstream adds features)
    extras = [c for c in df.columns if c not in final_cols]
    df = df[final_cols + extras]

    # OVERWRITE (no append)
    df.to_csv(out_csv, index=False)
    print(f"[append_player_history_from_prep] Wrote {len(df)} rows to {out_csv} (overwritten).")


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter & cap today's props; overwrite final file.")
    p.add_argument(
        "--prep-csv",
        default="data/props_prep.csv",
        help="Path to the input prep CSV (default: data/props_prep.csv)",
    )
    p.add_argument(
        "--out-csv",
        default="data/final_player_props.csv",
        help="Output CSV path to overwrite (default: data/final_player_props.csv)",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    run(prep_csv=args.prep_csv, out_csv=args.out_csv)


if __name__ == "__main__":
    main()
