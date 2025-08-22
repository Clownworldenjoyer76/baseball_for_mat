#!/usr/bin/env python3
"""
Append today's batter props from data/bets/prep/batter_props_final.csv
into data/bets/player_props_history.csv, mapping columns to the history schema
and handling column-name drift safely.
"""

from __future__ import annotations
from pathlib import Path
import datetime as dt
import pandas as pd
import numpy as np


PREP_FILE    = Path("data/bets/prep/batter_props_final.csv")
HISTORY_FILE = Path("data/bets/player_props_history.csv")

HISTORY_COLUMNS = [
    "player_id","name","team","prop","line","value",
    "over_probability","date","game_id","prop_correct","prop_sort"
]


def _pick_col(df: pd.DataFrame, names: list[str]) -> pd.Series:
    """Return the first existing column by name; else NA series."""
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([pd.NA] * len(df))


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _normalize_over_prob(s: pd.Series) -> pd.Series:
    """Coerce to [0,1]. If it looks like percent (0-100], scale down."""
    s = _to_num(s)
    if s.notna().any():
        # Scale values > 1 and <= 100 to 0-1
        scaled = s.copy()
        mask_pct = scaled > 1.0
        scaled.loc[mask_pct] = scaled.loc[mask_pct] / 100.0
        # Clamp
        scaled = scaled.clip(lower=0.01, upper=0.99)
        return scaled
    return s


def main() -> None:
    if not PREP_FILE.exists():
        print(f"❌ Missing prep file: {PREP_FILE}")
        return

    df = pd.read_csv(PREP_FILE)
    if df.empty:
        print("❌ Prep file is empty")
        return

    # Normalize columns to lowercase for flexible matching
    df.columns = [c.strip().lower() for c in df.columns]

    # Build the output in the history schema
    out = pd.DataFrame()

    # Core ID/name/team
    out["player_id"] = _pick_col(df, ["player_id", "id"]).astype("Int64")
    out["name"]      = _pick_col(df, ["player_name", "name"])
    out["team"]      = _pick_col(df, ["team", "team_name", "team_abbr", "team_code"])

    # Market / line
    out["prop"] = _pick_col(df, ["prop_type", "prop", "market"])
    out["line"] = _to_num(_pick_col(df, ["prop_line", "line"]))

    # Value (price/odds)
    val_series = _pick_col(df, ["value", "odds", "price"])
    out["value"] = _to_num(val_series)

    # Over probability — try several candidates
    over_prob = _pick_col(df, [
        "over_probability", "prob_over", "prob_over_yn", "over_prob",
        # common specific markets (keep last as fallbacks)
        "prob_hits_over_1p5", "prob_hr_over_0p5", "prob_tb_over_1p5"
    ])
    out["over_probability"] = _normalize_over_prob(over_prob)

    # Date (to YYYY-MM-DD string)
    # Try 'date' then common timestamp fields
    date_col = _pick_col(df, ["date", "asof", "timestamp", "pulled_at", "updated_at"])
    parsed_date = pd.to_datetime(date_col, errors="coerce").dt.date
    # Fallback to today if completely missing
    if parsed_date.isna().all():
        parsed_date = pd.Series([dt.date.today()] * len(df))
    out["date"] = parsed_date.astype(str)

    # Game id
    out["game_id"] = _pick_col(df, ["game_id"])

    # Placeholders
    out["prop_correct"] = pd.NA
    out["prop_sort"]    = _pick_col(df, ["prop_sort"])

    # Keep only today's rows
    today_str = str(dt.date.today())
    before = len(out)
    out = out[out["date"] == today_str].copy()
    print(f"Prep rows today: {len(out)} (from {before} total rows in prep)")

    if out.empty:
        print("❌ No rows for today in prep file after date filter; nothing to append.")
        # Ensure file exists with headers so downstream steps don't fail
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not HISTORY_FILE.exists():
            pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_FILE, index=False)
        return

    # Align to exact schema & types
    for col in HISTORY_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[HISTORY_COLUMNS]

    # Union with existing history and de-duplicate
    if HISTORY_FILE.exists():
        hist = pd.read_csv(HISTORY_FILE)
        combined = pd.concat([hist, out], ignore_index=True)
    else:
        combined = out

    # Dedup by identity of the offered bet for a given date
    dedup_keys = ["player_id", "prop", "line", "date", "game_id"]
    combined = combined.drop_duplicates(subset=dedup_keys, keep="last")

    # Write
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HISTORY_FILE, index=False)
    print(f"✅ Appended {len(out)} rows; history now {len(combined)} total rows at {HISTORY_FILE}")


if __name__ == "__main__":
    main()
