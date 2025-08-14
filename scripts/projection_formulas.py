# scripts/projection_formulas.py
# Header-aware projection utilities for batter stats.
# Computes AB from PA/BB%, normalizes rates, derives hits/HR per AB,
# and emits projections: proj_hits, proj_hr, proj_avg, proj_slg (+effective K%/BB%).

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Tuple, Optional
import pandas as pd
import numpy as np

# Reasonable clamps to keep rates within plausible MLB bounds.
CLIP_BOUNDS = {
    "bb_percent": (0.00, 0.20),
    "k_percent":  (0.05, 0.40),
    "hr_per_ab":  (0.00, 0.12),
    "hits_per_ab":(0.15, 0.40),
    "xbh_share":  (0.05, 0.60),
    "1b_share":   (0.30, 0.95),
    "bb_per_pa":  (0.00, 0.20),
}

# Column aliases we can accept from various inputs.
ALIASES = {
    "pa": ["pa", "PA"],
    "ab": ["ab", "AB"],

    "bb_percent": ["bb_percent", "BB%", "bb_rate"],
    "k_percent":  ["k_percent", "K%", "k_rate"],

    # hits-per-AB proxy (batting average often stands in for H/AB)
    "hits_per_ab": ["hits_per_ab", "H/AB", "hit_rate_ab", "avg", "AVG", "batting_avg"],

    # explicit HR rate per AB; else derive from counts
    "hr_per_ab": ["hr_per_ab", "HR/AB", "hr_rate_ab"],

    # opponent preference (if present, can override batter-side rates)
    "opp_k_percent": ["opp_k_percent", "opp_K%", "opponent_k_percent"],
    "opp_bb_percent": ["opp_bb_percent", "opp_BB%", "opponent_bb_percent"],

    # counts for derivations when rates are missing
    "hits":       ["hit", "hits", "H"],
    "hr":         ["home_run", "HR"],
    "walks":      ["walk", "BB"],
    "strikeouts": ["strikeout", "SO", "K"],

    # optional helpers for SLG decomposition
    "xbh_share": ["xbh_share"],
    "1b_share":  ["1b_share", "singles_share"],
}

REQUIRED_BASE = ["pa"]  # AB will be computed from PA & BB%/walks if needed

# When opponent columns exist, prefer them as effective rates.
OPP_PITCHER_PREF = {
    "k_percent":  "opp_k_percent",
    "bb_percent": "opp_bb_percent",
}

# ------------------------- helpers -------------------------

def _first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """Return the first column name found among candidates (case-insensitive)."""
    for c in candidates:
        if c in df.columns:
            return c
        for col in df.columns:
            if col.lower() == c.lower():
                return col
   
