#!/usr/bin/env python3
"""
Append today's batter props (prep) into the canonical player history.

Fixes:
- over_probability is mapped from the prop-specific probability columns
- dedupe does NOT include over_probability (keeps newest row only)
- prop_sort ranks props by highest probability per (date, player_id)
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import pytz

PREP_PATH = Path("data/bets/prep/batter_props_final.csv")
HIST_PATH = Path("data/bets/player_props_history.csv")
TZ = pytz.timezone("America/New_York")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()

    # Strings
    for c in ["prop", "team", "opp_team", "name", "opp_pitcher_name"]:
        if c in out.columns:
            out[c] = out[c].astype("string").str.strip().str.lower()

    # IDs / numerics
    if "player_id" in out.columns:
        out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")

    num_cols = [
        "line",
        "value",
        "batter_z",
        "mega_z",
        "opp_pitcher_z",
        "opp_pitcher_mega_z",
        "prob_hits_over_1p5",
        "prob_tb_over_1p5",
        "prob_hr_over_0p5",
        "over_probability",  # might exist; we will overwrite from mapping below
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # Timestamp/date
    now_local = datetime.now(TZ)
    out["timestamp"] = now_local.isoformat(timespec="seconds")
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype("string")
    else:
        out["date"] = now_local.date().isoformat()

    return out


def _derive_over_probability(df: pd.DataFrame) -> pd.DataFrame:
    """
    Choose the correct probability column based on 'prop'.
    - hits         -> prob_hits_over_1p5
    - total_bases  -> prob_tb_over_1p5
    - home_runs    -> prob_hr_over_0p5
    Fallback: existing 'over_probability' if present.
    """
    if df.empty:
        return df.copy()

    out = df.copy()
    prop_col = out.get("prop")
    if prop_col is None:
        # Nothing to map; keep whatever exists or NaN
        if "over_probability" not in out.columns:
            out["over_probability"] = np.nan
        return out

    # Build a mapped series initialized as NaN
    mapped = pd.Series(np.nan, index=out.index, dtype="float64")

    mapping = {
        "hits": "prob_hits_over_1p5",
        "total_bases": "prob_tb_over_1p5",
        "home_runs": "prob_hr_over_0p5",
        "hr": "prob_hr_over_0p5",  # just in case
    }

    for prop_name, prob_col in mapping.items():
        mask = out["prop"] == prop_name
        if prob_col in out.columns:
            mapped.loc[mask] = pd.to_numeric(out.loc[mask, prob_col], errors="coerce")

    # Fallback to any preexisting over_probability where still NaN
    if "over_probability" in out.columns:
        mapped = mapped.fillna(pd.to_numeric(out["over_probability"], errors="coerce"))

    out["over_probability"] = mapped

    return out


def _compute_prop_sort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank DESC by over_probability within (date, player_id).
    Ties broken by:
      1) higher 'value'
      2) 'prop' alphabetical (stable, deterministic)
    Dense ranking -> 1,2,3...
    """
    if df.empty:
        return df.copy()

    out = df.copy()
    if "over_probability" not in out.columns:
        out["over_probability"] = np.nan

    # Fill NaN with -1 so they sink
    out["_prob_sort_key"] = out["over_probability"].fillna(-1)

    by = [c for c in ["date", "player_id"] if c in out.columns]
    if not by:
        # Global rank if keys missing
        order = out.sort_values(
            ["_prob_sort_key", "value", "prop"],
            ascending=[False, False, True],
            kind="mergesort",
        )
        out.loc[order.index, "prop_sort"] = (
            (-order["_prob_sort_key"])
            .rank(method="dense", ascending=False)
            .astype("Int64")
        )
        out.drop(columns=["_prob_sort_key"], inplace=True)
        return out

    # groupwise stable sort then dense rank
    order = out.sort_values(
        by + ["_prob_sort_key", "value", "prop"],
        ascending=[True] * len(by) + [False, False, True],
        kind="mergesort",
    )
    # within groups, assign dense rank by probability (desc)
    out.loc[order.index, "prop_sort"] = (
        order.groupby(by)["_prob_sort_key"]
        .rank(method="dense", ascending=False)
        .astype("Int64")
    )

    out.drop(columns=["_prob_sort_key"], inplace=True)
    return out


def _align_columns(a: pd.DataFrame, b: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    cols = sorted(set(a.columns) | set(b.columns))
    return a.reindex(columns=cols), b.reindex(columns=cols)


def _drop_dupes_keep_newest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stable identity for a prop row:
      date, player_id, team, prop, line, value
    Keep the last (newest timestamp).
    """
    if df.empty:
        return df
    keys = [c for c in ["date", "player_id", "team", "prop", "line", "value"] if c in df.columns]
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")
    return df.drop_duplicates(subset=keys, keep="last")


def main():
    prep = _read_csv(PREP_PATH)
    if prep.empty:
        print(f"[append] Prep file missing or empty: {PREP_PATH}")
        return

    # Normalize + derive correct probability
    prep = _normalize(prep)
    prep = _derive_over_probability(prep)

    # Rank props by highest probability per player/day
    prep = _compute_prop_sort(prep)

    # Read history and merge
    hist = _read_csv(HIST_PATH)
    hist, prep = _align_columns(hist, prep)

    combined = pd.concat([hist, prep], ignore_index=True)

    # Deduplicate WITHOUT over_probability so only newest record remains
    combined = _drop_dupes_keep_newest(combined)

    # Recompute prop_sort again on the final set (in case dedupe changed rows)
    combined = _compute_prop_sort(combined)

    HIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HIST_PATH, index=False)
    print(f"[append] Wrote {len(combined):,} rows to {HIST_PATH}")


if __name__ == "__main__":
    main()
