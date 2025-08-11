
# projection_formulas.py
# Updated to:
# - Use AB (not PA) for Hits, HR, and SLG scaling
# - Derive AB from PA and BB% (AB = PA * (1 - BB_rate))
# - Prefer opponent pitcher stats (e.g., opp_k_percent) when available
# - Clip/normalize rates to realistic MLB ranges
# - Fail fast on missing required columns (no silent defaults)

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Tuple
import pandas as pd
import numpy as np

# ---- Configuration ----

# Realistic MLB ranges (conservative, season-agnostic)
CLIP_BOUNDS = {
    # rates in decimal space
    "bb_percent": (0.00, 0.20),       # 0–20%
    "k_percent": (0.05, 0.40),        # 5–40%
    "hr_per_ab": (0.00, 0.12),        # 0–12% of AB end in HR
    "hits_per_ab": (0.15, 0.40),      # 15–40% hit rate (batting avg proxy)
    "xbh_share": (0.05, 0.60),        # fraction of hits that are XBH
    "1b_share": (0.30, 0.95),         # fraction of hits that are singles
    "bb_per_pa": (0.00, 0.20),        # same as bb_percent alias
}

# Column aliases to tolerate common variants, all converted to standardized names
ALIASES = {
    "pa": ["pa", "PA"],
    "bb_percent": ["bb_percent", "BB%", "bb_rate"],
    "k_percent": ["k_percent", "K%", "k_rate"],
    "hr_per_ab": ["hr_per_ab", "HR/AB", "hr_rate_ab"],
    "hits_per_ab": ["hits_per_ab", "H/AB", "hit_rate_ab", "avg", "AVG"],
    # Opponent pref columns
    "opp_k_percent": ["opp_k_percent", "opp_K%", "opponent_k_percent"],
    "opp_bb_percent": ["opp_bb_percent", "opp_BB%", "opponent_bb_percent"],
    # Useful supporting rates if provided
    "xbh_share": ["xbh_share"],
    "1b_share": ["1b_share", "singles_share"],
}

REQUIRED_BASE = ["pa"]  # we will compute AB after resolving bb%
REQUIRED_RATES = ["bb_percent", "k_percent", "hits_per_ab"]  # baseline batter rates

OPP_PITCHER_PREF = {
    "k_percent": "opp_k_percent",
    "bb_percent": "opp_bb_percent",
}

# ---- Helpers ----

def _first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
        # also try case-insensitive exact match
        for col in df.columns:
            if col.lower() == c.lower():
                return col
    return None

def _require_columns(df: pd.DataFrame, logical_names: Iterable[str]) -> List[Tuple[str, str]]:
    """
    Ensure each logical column exists (via alias). Return list of (logical_name, resolved_column).
    Raise ValueError listing any missing.
    """
    resolved = []
    missing = []
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
    if low is None:
        return s
    return s.clip(lower=low, upper=high)

def _normalize_rate_smart(name: str, s: pd.Series) -> pd.Series:
    """Accept either percent (e.g., 12 for 12%) or decimal (0.12) and normalize to decimal, then clip."""
    s_norm = s.astype(float)
    # Heuristic: if any value > 1, treat as percent
    if (s_norm.abs() > 1).any():
        s_norm = s_norm / 100.0
    return _clip_series(name, s_norm)

def _prefer_opponent(df: pd.DataFrame, logical_name: str, fallback_col: str) -> pd.Series:
    """Return opponent metric if present, else fallback batter metric (already normalized/decimal)."""
    opp_logical = OPP_PITCHER_PREF.get(logical_name)
    if not opp_logical:
        return df[fallback_col]
    # Try resolve opponent column
    opp_aliases = ALIASES.get(opp_logical, [opp_logical])
    opp_col = _first_existing(df, opp_aliases)
    if opp_col and opp_col in df.columns:
        # normalize opponent rate, then clip using same bounds for logical_name
        norm_name = logical_name  # reuse bounds for logical metric
        return _normalize_rate_smart(norm_name, df[opp_col])
    return df[fallback_col]

def _ensure_ab_column(df: pd.DataFrame, pa_col: str, bb_col: str) -> pd.Series:
    """Compute AB = PA * (1 - BB%) using normalized bb%."""
    bb_dec = _normalize_rate_smart("bb_per_pa", df[bb_col])
    pa = df[pa_col].astype(float).clip(lower=0)
    ab = (pa * (1.0 - bb_dec)).round(3)
    # prevent divide-by-zero later
    ab = ab.clip(lower=0.0)
    return ab

# ---- Public API ----

@dataclass(frozen=True)
class ProjectionConfig:
    # Future extensibility hook if needed
    pass

def calculate_all_projections(df: pd.DataFrame, config: ProjectionConfig | None = None) -> pd.DataFrame:
    """
    Main entrypoint. Expects raw batter-level inputs and (optionally) opponent pitcher context.
    Enforces presence of required cols, computes AB, and projects key outputs.

    Outputs (added columns):
      - AB (derived)
      - proj_hits, proj_hr (scaled by AB)
      - proj_avg (proxy from hits_per_ab)
      - proj_slg (from simple TB model, scaled by AB)
      - k_percent_eff, bb_percent_eff (opponent-adjusted, clipped)
    """
    # 1) Resolve required inputs
    resolved_base = dict(_require_columns(df, REQUIRED_BASE))
    resolved_rates = dict(_require_columns(df, REQUIRED_RATES))

    pa_col = resolved_base["pa"]
    bb_col = resolved_rates["bb_percent"]
    k_col = resolved_rates["k_percent"]
    hits_ab_col = resolved_rates["hits_per_ab"]

    # 2) Normalize batter-side rates to decimal + clipped
    bb_dec = _normalize_rate_smart("bb_percent", df[bb_col])
    k_dec = _normalize_rate_smart("k_percent", df[k_col])
    hits_per_ab = _normalize_rate_smart("hits_per_ab", df[hits_ab_col])

    # 3) Opponent preference override (if available)
    # For k% and bb%, prefer opponent-pitcher versions; hits_per_ab remains batter-driven
    k_dec_eff = _prefer_opponent(pd.DataFrame({k_col: k_dec, **{c: df[c] for c in df.columns}}), "k_percent", k_col)
    bb_dec_eff = _prefer_opponent(pd.DataFrame({bb_col: bb_dec, **{c: df[c] for c in df.columns}}), "bb_percent", bb_col)

    # 4) Compute AB = PA * (1 - BB%)
    ab = _ensure_ab_column(pd.DataFrame({pa_col: df[pa_col], bb_col: df[bb_col]}), pa_col, bb_col)
    df = df.copy()
    df["AB"] = ab

    # 5) HR rate per AB, if provided; else approximate as share of hits (very conservative), then clip
    hr_rate_ab_col = _first_existing(df, ALIASES["hr_per_ab"])  # optional
    if hr_rate_ab_col:
        hr_per_ab = _normalize_rate_smart("hr_per_ab", df[hr_rate_ab_col])
    else:
        # fallback: extremely conservative estimate based on hits_per_ab
        hr_per_ab = _clip_series("hr_per_ab", 0.12 * hits_per_ab)  # cap inside _clip_series

    # 6) Projected counting stats (AB-scaled)
    df["proj_hits"] = (hits_per_ab * df["AB"]).round(3)
    df["proj_hr"] = (hr_per_ab * df["AB"]).round(3)
    df["proj_avg"] = hits_per_ab.round(3)

    # 7) Simple SLG model:
    #   - Estimate singles share vs XBH; if not provided, derive from hits_per_ab and HR rate
    xbh_share_col = _first_existing(df, ALIASES["xbh_share"])
    oneb_share_col = _first_existing(df, ALIASES["1b_share"])

    if xbh_share_col:
        xbh_share = _normalize_rate_smart("xbh_share", df[xbh_share_col])
    else:
        # heuristic: higher HR rate -> higher XBH share
        xbh_share = _clip_series("xbh_share", (hr_per_ab / hits_per_ab.replace(0, np.nan)).fillna(0).clip(0, 0.6) + 0.1)

    if oneb_share_col:
        oneb_share = _normalize_rate_smart("1b_share", df[oneb_share_col])
    else:
        oneb_share = 1.0 - xbh_share
        oneb_share = _clip_series("1b_share", oneb_share)

    # Approximate TB per AB:
    #   TB/AB = 1B*1 + 2B*2 + 3B*3 + HR*4, where shares are of all AB outcomes.
    #   Use a simple mix: assume XBH split 2B:3B:(HR remainder) = 0.65 : 0.10 : (rest)
    #   HR share is hr_per_ab; ensure it's part of XBH
    singles_per_ab = (oneb_share * hits_per_ab).clip(0, 1)
    xbh_per_ab = (xbh_share * hits_per_ab).clip(0, 1)
    hr_per_ab = hr_per_ab.clip(0, xbh_per_ab)  # HR cannot exceed total XBH
    rem_xbh = (xbh_per_ab - hr_per_ab).clip(lower=0.0)
    doubles_per_ab = 0.65 * rem_xbh
    triples_per_ab = 0.10 * rem_xbh

    tb_per_ab = (singles_per_ab * 1.0 +
                 doubles_per_ab * 2.0 +
                 triples_per_ab * 3.0 +
                 hr_per_ab * 4.0)

    # SLG is TB / AB; since tb_per_ab is in per-AB space, SLG == tb_per_ab
    df["proj_slg"] = tb_per_ab.round(3)

    # 8) Expose opponent-adjusted plate discipline
    df["k_percent_eff"] = _clip_series("k_percent", pd.to_numeric(k_dec_eff, errors="coerce")).round(4)
    df["bb_percent_eff"] = _clip_series("bb_percent", pd.to_numeric(bb_dec_eff, errors="coerce")).round(4)

    # 9) Final sanity: replace any inf / nan introduced by divide-by-zero with zeros (post-clipping)
    for c in ["proj_hits", "proj_hr", "proj_avg", "proj_slg"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).replace([np.inf, -np.inf], 0)

    return df
