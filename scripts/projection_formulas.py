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
    return None

def _require_columns(df: pd.DataFrame, logical_names: Iterable[str]) -> List[Tuple[str, str]]:
    """Resolve logical names to actual columns; error if missing."""
    resolved, missing = [], []
    for name in logical_names:
        aliases = ALIASES.get(name, [name])
        col = _first_existing(df, aliases)
        if col is None:
            missing.append(name)
        else:
            resolved.append((name, col))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    return resolved

def _clip_series(name: str, s: pd.Series) -> pd.Series:
    low, high = CLIP_BOUNDS.get(name, (None, None))
    return s.clip(lower=low, upper=high) if low is not None else s

def _normalize_rate_smart(name: str, s: pd.Series) -> pd.Series:
    """
    Convert % forms (0–100) to decimals (0–1) when needed, coerce to numeric,
    fill NaNs with 0, and clamp to plausible bounds.
    """
    s_norm = pd.to_numeric(s, errors="coerce")
    if (s_norm.abs() > 1).any():
        s_norm = s_norm / 100.0
    return _clip_series(name, s_norm.fillna(0))

def _prefer_opponent(df: pd.DataFrame, logical_name: str, fallback_col: str) -> pd.Series:
    """
    Prefer opponent rate if present; otherwise use fallback column (already normalized).
    Expect df to contain either opponent column or the fallback series under fallback_col.
    """
    opp_logical = OPP_PITCHER_PREF.get(logical_name)
    if not opp_logical:
        return df[fallback_col]
    opp_aliases = ALIASES.get(opp_logical, [opp_logical])
    opp_col = _first_existing(df, opp_aliases)
    if opp_col and opp_col in df.columns:
        return _normalize_rate_smart(logical_name, df[opp_col])
    return df[fallback_col]

def _ensure_ab_from_pa_bb(df: pd.DataFrame, pa_col: str) -> pd.Series:
    """
    AB ≈ PA * (1 - BB%), with BB% derived from column or walks/PA if needed.
    """
    bb_col = _first_existing(df, ALIASES["bb_percent"])
    if bb_col is None:
        walk_col = _first_existing(df, ALIASES["walks"])
        if walk_col is None:
            raise ValueError("Missing BB% and walks; cannot compute AB from PA.")
        bb_dec = _normalize_rate_smart(
            "bb_percent",
            pd.to_numeric(df[walk_col], errors="coerce") /
            pd.to_numeric(df[pa_col],  errors="coerce").replace(0, np.nan)
        )
    else:
        bb_dec = _normalize_rate_smart("bb_percent", df[bb_col])

    pa = pd.to_numeric(df[pa_col], errors="coerce").fillna(0).clip(lower=0)
    ab = (pa * (1.0 - bb_dec)).clip(lower=0).round(3)
    return ab

def _derive_hits_per_ab(df: pd.DataFrame, ab_series: pd.Series) -> pd.Series:
    hits_col = _first_existing(df, ALIASES["hits"])
    if hits_col is None:
        raise ValueError("Missing hits-per-AB proxy and raw hits; cannot derive hits_per_ab.")
    ab_safe = ab_series.replace(0, np.nan)
    hits = pd.to_numeric(df[hits_col], errors="coerce").fillna(0)
    h_ab = (hits / ab_safe).fillna(0)
    return _clip_series("hits_per_ab", h_ab)

def _derive_hr_per_ab(df: pd.DataFrame, ab_series: pd.Series) -> Optional[pd.Series]:
    hr_col = _first_existing(df, ALIASES["hr"])
    if hr_col is None:
        return None
    ab_safe = ab_series.replace(0, np.nan)
    hr = pd.to_numeric(df[hr_col], errors="coerce").fillna(0)
    hr_ab = (hr / ab_safe).fillna(0)
    return _clip_series("hr_per_ab", hr_ab)

# ------------------------- public API -------------------------

@dataclass(frozen=True)
class ProjectionConfig:
    """Placeholder for future tunables."""
    pass

def calculate_all_projections(
    df: pd.DataFrame,
    config: ProjectionConfig | None = None
) -> pd.DataFrame:
    """
    Given batter-level inputs (header-flexible), returns a DataFrame with:
      - AB (derived)
      - proj_hits (expected hits)
      - proj_hr   (expected HR)
      - proj_avg  (H/AB proxy)
      - proj_slg  (per-AB total bases proxy)
      - k_percent_eff, bb_percent_eff (effective rates, opp-adjusted if present)

    Assumptions:
      - If rates are provided as percentages (0–100), they’re normalized to 0–1.
      - If rates are missing, derives them from counts when possible.
    """
    # Work on a copy; keep original columns intact
    df = df.copy()
    # Resolve required base columns
    resolved_base = dict(_require_columns(df, REQUIRED_BASE))
    pa_col = resolved_base["pa"]

    # AB from PA & BB%
    df["AB"] = _ensure_ab_from_pa_bb(df, pa_col)

    # K%: use rate if present, else derive from strikeouts / PA
    k_col = _first_existing(df, ALIASES["k_percent"])
    if k_col is not None:
        k_dec = _normalize_rate_smart("k_percent", df[k_col])
    else:
        so_col = _first_existing(df, ALIASES["strikeouts"])
        if so_col is None:
            raise ValueError("Missing K% and strikeouts; cannot proceed.")
        k_dec = _normalize_rate_smart(
            "k_percent",
            pd.to_numeric(df[so_col], errors="coerce") /
            pd.to_numeric(df[pa_col], errors="coerce").replace(0, np.nan)
        )

    # BB%: use rate if present, else derive from walks / PA
    bb_col = _first_existing(df, ALIASES["bb_percent"])
    if bb_col is not None:
        bb_dec = _normalize_rate_smart("bb_percent", df[bb_col])
    else:
        walk_col = _first_existing(df, ALIASES["walks"])
        if walk_col is None:
            raise ValueError("Missing BB% and walks; cannot proceed.")
        bb_dec = _normalize_rate_smart(
            "bb_percent",
            pd.to_numeric(df[walk_col], errors="coerce") /
            pd.to_numeric(df[pa_col],  errors="coerce").replace(0, np.nan)
        )

    # Opponent overrides (if present). Build small frames to pass to _prefer_opponent.
    k_df  = pd.DataFrame({k_col or "k": k_dec})
    bb_df = pd.DataFrame({bb_col or "bb": bb_dec})
    k_dec_eff  = _prefer_opponent(pd.concat([k_df, df], axis=1), "k_percent",  k_col or "k")
    bb_dec_eff = _prefer_opponent(pd.concat([bb_df, df], axis=1), "bb_percent", bb_col or "bb")

    # Hits per AB: use column if present, else derive from counts
    h_ab_col = _first_existing(df, ALIASES["hits_per_ab"])
    if h_ab_col is not None:
        hits_per_ab = _normalize_rate_smart("hits_per_ab", df[h_ab_col])
    else:
        hits_per_ab = _derive_hits_per_ab(df, df["AB"])

    # HR per AB: use column if present; else derive from counts; else conservative share of hits
    hr_ab_col = _first_existing(df, ALIASES["hr_per_ab"])
    if hr_ab_col is not None:
        hr_per_ab = _normalize_rate_smart("hr_per_ab", df[hr_ab_col])
    else:
        derived_hr = _derive_hr_per_ab(df, df["AB"])
        if derived_hr is not None:
            hr_per_ab = derived_hr
        else:
            # Conservative mapping if only AVG present: a fraction of hits are HR
            hr_per_ab = _clip_series("hr_per_ab", 0.12 * hits_per_ab)

    # Projections (AB-scaled expectations)
    df["proj_hits"] = (hits_per_ab * df["AB"]).round(3)
    df["proj_hr"]   = (hr_per_ab   * df["AB"]).round(3)
    df["proj_avg"]  = hits_per_ab.round(3)

    # Simple SLG proxy via decomposition into 1B/2B/3B/HR shares
    xbh_share_col = _first_existing(df, ALIASES["xbh_share"])
    oneb_share_col = _first_existing(df, ALIASES["1b_share"])
    if xbh_share_col:
        xbh_share = _normalize_rate_smart("xbh_share", df[xbh_share_col])
    else:
        # Heuristic: HR are a subset of XBH; add small baseline if unknown
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = (hr_per_ab / hits_per_ab.replace(0, np.nan)).fillna(0)
        xbh_share = _clip_series("xbh_share", ratio.clip(0, 0.6) + 0.1)

    if oneb_share_col:
        oneb_share = _normalize_rate_smart("1b_share", df[oneb_share_col])
    else:
        oneb_share = _clip_series("1b_share", 1.0 - xbh_share)

    singles_per_ab = (oneb_share * hits_per_ab).clip(0, 1)
    xbh_per_ab     = (xbh_share  * hits_per_ab).clip(0, 1)
    hr_per_ab      = hr_per_ab.clip(0, xbh_per_ab)

    rem_xbh = (xbh_per_ab - hr_per_ab).clip(lower=0.0)
    doubles_per_ab = 0.65 * rem_xbh
    triples_per_ab = 0.10 * rem_xbh

    tb_per_ab = singles_per_ab*1 + doubles_per_ab*2 + triples_per_ab*3 + hr_per_ab*4
    df["proj_slg"] = tb_per_ab.round(3)

    # Effective (possibly opponent-adjusted) displayed rates
    df["k_percent_eff"]  = _clip_series("k_percent",  pd.to_numeric(k_dec_eff,  errors="coerce")).round(4)
    df["bb_percent_eff"] = _clip_series("bb_percent", pd.to_numeric(bb_dec_eff, errors="coerce")).round(4)

    # Safety: coerce projections to finite numbers
    for c in ["proj_hits", "proj_hr", "proj_avg", "proj_slg"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).replace([np.inf, -np.inf], 0)

    return df

# Optional explicit export for linting/static tools
__all__ = [
    "ProjectionConfig",
    "calculate_all_projections",
]
