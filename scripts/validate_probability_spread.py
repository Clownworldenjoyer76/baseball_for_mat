
# scripts/validate_probability_spread.py
"""
Lightweight validator used in CI to catch obviously broken projections,
without forcing artificial clipping. It:
- loads data/_projections/batter_props_projected.csv
- finds columns prob_*
- prints basic stats
- exits non-zero only if there are no numeric values at all OR
  if *every* prob column is completely constant across all rows (e.g., all 0s or all 1s).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


IN_PATH = Path("data/_projections/batter_props_projected.csv")


def main() -> int:
    if not IN_PATH.exists():
        print(f"❌ Missing {IN_PATH}")
        return 2

    df = pd.read_csv(IN_PATH)
    prob_cols = [c for c in df.columns if c.startswith("prob_")]
    if not prob_cols:
        print("❌ No prob_* columns found.")
        return 2

    probs = pd.concat([pd.to_numeric(df[c], errors="coerce") for c in prob_cols], axis=1)
    probs = probs.dropna(how="all")
    if probs.empty:
        print("❌ All prob_* values are NaN / non-numeric.")
        return 2

    # Print summary
    print("Probability columns:", ", ".join(prob_cols))
    desc = probs.describe()
    print(desc)

    # Failure conditions
    all_constant = True
    for c in prob_cols:
        s = probs[c].dropna()
        if s.empty:
            continue
        if s.nunique(dropna=True) > 1:
            all_constant = False
            break

    if all_constant:
        print("❌ All probability columns are constant. Upstream likely broken.")
        return 1

    # Otherwise OK
    print("✅ Probability spread looks sane enough for downstream steps.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
