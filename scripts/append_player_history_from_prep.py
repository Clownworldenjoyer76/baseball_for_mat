#!/usr/bin/env python3
# scripts/append_player_history_from_prep.py
"""
Append today's prep batter props into the running player props history.

- Reads:  data/bets/prep/batter_props_final.csv
- Appends to (creating if needed): data/bets/player_props_history.csv
- Normalizes key text fields (e.g., prop) safely using .str accessors.
- Adds local timestamp (America/New_York) and a date column.
- Aligns columns between prep and history and drops perfect duplicates.

Exit code:
  0 on success (including "nothing to do")
  1 on unexpected error
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
import pandas as pd

# ---------- Paths ----------
PREP_PATH = Path("data/bets/prep/batter_props_final.csv")
HIST_PATH = Path("data/bets/player_props_history.csv")

# ---------- Helpers ----------

def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[ERROR] Failed to read {path}: {e}", file=sys.stderr)
        raise

def _normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    df = df.copy()

    # Standardize column names (lowercase; single underscores)
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^0-9a-zA-Z_]", "_", regex=True)
        .str.lower()
    )

    # Normalize "prop" text safely (THIS was the line causing the .strip error)
    if "prop" in df.columns:
        df["prop"] = df["prop"].astype(str).str.strip().str.lower()

    # Common numeric columns to coerce
    for col in [
        "player_id",
        "line",
        "value",
        "over_probability",
        "under_probability",
        "prob_hits_over_1p5",
        "prob_tb_over_1p5",
        "prob_hr_over_0p5",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Name/team tidy
    for col in ["name", "team"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df

def _add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add timestamp (local NY) and date if not present."""
    if df.empty:
        return df

    df = df.copy()
    # Respect the repo-wide TZ env if present (workflow sets TZ=America/New_York)
    tz = os.environ.get("TZ", "America/New_York")
    now_local = pd.Timestamp.now(tz=tz)
    if "timestamp" not in df.columns:
        df["timestamp"] = now_local.isoformat()
    if "date" not in df.columns:
        df["date"] = now_local.date().isoformat()
    return df

def _align_columns(prep: pd.DataFrame, hist: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Union columns so we can concat without losing fields."""
    cols_union = sorted(set(prep.columns).union(hist.columns))
    return prep.reindex(columns=cols_union), hist.reindex(columns=cols_union)

def _drop_dupes(df: pd.DataFrame) -> pd.DataFrame:
    """Drop exact duplicate rows. If you want a stricter key, adjust subset."""
    if df.empty:
        return df
    # A sensible de-duplication set (covers typical uniqueness for a day)
    subset = [c for c in [
        "date", "player_id", "name", "team",
        "prop", "line", "value", "over_probability", "under_probability"
    ] if c in df.columns]
    return df.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)

# ---------- Main ----------

def main() -> int:
    # 1) Load prep
    if not PREP_PATH.exists():
        print(f"[INFO] {PREP_PATH} not found — nothing to append. Exiting 0.")
        return 0

    prep = _read_csv_safe(PREP_PATH)
    if prep.empty:
        print(f"[INFO] {PREP_PATH} is empty — nothing to append. Exiting 0.")
        return 0

    prep = _normalize_ids(prep)
    prep = _add_time_columns(prep)

    # 2) Load existing history (if any)
    hist = _read_csv_safe(HIST_PATH)
    if not hist.empty:
        hist = _normalize_ids(hist)
        # If history is missing time columns, add them (keeps old timestamps intact)
        hist = _add_time_columns(hist)

    # 3) Align columns
    prep, hist = _align_columns(prep, hist)

    # 4) Append and de-duplicate
    before_hist_rows = len(hist)
    combined = pd.concat([hist, prep], ignore_index=True)
    combined = _drop_dupes(combined)

    added_rows = len(combined) - before_hist_rows
    # 5) Ensure parent dir exists and write
    HIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HIST_PATH, index=False)

    # 6) Small report
    print(f"[OK] Appended {added_rows} new row(s) from prep into history.")
    print(f"[OK] Wrote {len(combined)} total rows -> {HIST_PATH}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit as e:
        raise
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
