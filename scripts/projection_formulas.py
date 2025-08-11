
# projection_formulas.py — header-aware + robust derivations
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Tuple
import pandas as pd
import numpy as np

CLIP_BOUNDS = {
    "bb_percent": (0.00, 0.20),
    "k_percent": (0.05, 0.40),
    "hr_per_ab": (0.00, 0.12),
    "hits_per_ab": (0.15, 0.40),
    "xbh_share": (0.05, 0.60),
    "1b_share": (0.30, 0.95),
    "bb_per_pa": (0.00, 0.20),
}

ALIASES = {
    "pa": ["pa", "PA"],
    "ab": ["ab", "AB"],
    "bb_percent": ["bb_percent", "BB%", "bb_rate"],
    "k_percent": ["k_percent", "K%", "k_rate"],
    # hits-per-AB proxy: your file uses 'batting_avg'
    "hits_per_ab": ["hits_per_ab", "H/AB", "hit_rate_ab", "avg", "AVG", "batting_avg"],
    # optional explicit HR rate per AB; else derive from HR & AB if present
    "hr_per_ab": ["hr_per_ab", "HR/AB", "hr_rate_ab"],
    # opponent preference
    "opp_k_percent": ["opp_k_percent", "opp_K%", "opponent_k_percent"],
    "opp_bb_percent": ["opp_bb_percent", "opp_BB%", "opponent_bb_percent"],
    # counts (for derivation if rates missing)
    "hits": ["hit", "hits", "H"],
    "hr": ["home_run", "HR"],
    "walks": ["walk", "BB"],
    "strikeouts": ["strikeout", "SO", "K"],
    # optional helpers
    "xbh_share": ["xbh_share"],
    "1b_share": ["1b_share", "singles_share"],
}

REQUIRED_BASE = ["pa"]  # AB will be computed from PA & BB%
REQUIRED_COUNTS_OR_RATES = ["bb_percent", "k_percent"]  # if absent, we will derive from counts

OPP_PITCHER_PREF = {
    "k_percent": "opp_k_percent",
    "bb_percent": "opp_bb_percent",
}

def _first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
        for col in df.columns:
            if col.lower() == c.lower():
                return col
    return None

def _require_columns(df: pd.DataFrame, logical_names: Iterable[str]) -> List[Tuple[str, str]]:
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
    s_norm = pd.to_numeric(s, errors="coerce")
    if (s_norm.abs() > 1).any():
        s_norm = s_norm / 100.0
    return _clip_series(name, s_norm.fillna(0))

def _prefer_opponent(df: pd.DataFrame, logical_name: str, fallback_col: str) -> pd.Series:
    opp_logical = OPP_PITCHER_PREF.get(logical_name)
    if not opp_logical:
        return df[fallback_col]
    opp_aliases = ALIASES.get(opp_logical, [opp_logical])
    opp_col = _first_existing(df, opp_aliases)
    if opp_col and opp_col in df.columns:
        return _normalize_rate_smart(logical_name, df[opp_col])
    return df[fallback_col]

def _ensure_ab_from_pa_bb(df: pd.DataFrame, pa_col: str) -> pd.Series:
    # Find/derive BB%
    bb_col = _first_existing(df, ALIASES["bb_percent"])
    if bb_col is None:
        # derive from counts: walks / PA
        walk_col = _first_existing(df, ALIASES["walks"])
        if walk_col is None:
            raise ValueError("Missing BB% and walks; cannot compute AB from PA.")
        bb_dec = _normalize_rate_smart("bb_percent", pd.to_numeric(df[walk_col], errors="coerce") / pd.to_numeric(df[pa_col], errors="coerce").replace(0, np.nan))
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

def _derive_hr_per_ab(df: pd.DataFrame, ab_series: pd.Series) -> pd.Series:
    hr_col = _first_existing(df, ALIASES["hr"])
    if hr_col is None:
        # If no HR counts, fall back to conservative fraction of hits_per_ab later
        return None
    ab_safe = ab_series.replace(0, np.nan)
    hr = pd.to_numeric(df[hr_col], errors="coerce").fillna(0)
    hr_ab = (hr / ab_safe).fillna(0)
    return _clip_series("hr_per_ab", hr_ab)

@dataclass(frozen=True)
class ProjectionConfig:
    pass

def calculate_all_projections(df: pd.DataFrame, config: ProjectionConfig | None = None) -> pd.DataFrame:
    resolved_base = dict(_require_columns(df, REQUIRED_BASE))
    pa_col = resolved_base["pa"]

    # Ensure AB from PA & BB%
    df = df.copy()
    df["AB"] = _ensure_ab_from_pa_bb(df, pa_col)

    # K%: use column if present else derive from strikeouts/PA
    k_col = _first_existing(df, ALIASES["k_percent"])
    if k_col is not None:
        k_dec = _normalize_rate_smart("k_percent", df[k_col])
    else:
        so_col = _first_existing(df, ALIASES["strikeouts"])
        if so_col is None:
            raise ValueError("Missing K% and strikeouts; cannot proceed.")
        k_dec = _normalize_rate_smart("k_percent", pd.to_numeric(df[so_col], errors="coerce") / pd.to_numeric(df[pa_col], errors="coerce").replace(0, np.nan))

    # BB% effective: prefer opponent if provided
    bb_col = _first_existing(df, ALIASES["bb_percent"])
    if bb_col is not None:
        bb_dec = _normalize_rate_smart("bb_percent", df[bb_col])
    else:
        walk_col = _first_existing(df, ALIASES["walks"])
        if walk_col is None:
            raise ValueError("Missing BB% and walks; cannot proceed.")
        bb_dec = _normalize_rate_smart("bb_percent", pd.to_numeric(df[walk_col], errors="coerce") / pd.to_numeric(df[pa_col], errors="coerce").replace(0, np.nan))

    # Opponent overrides
    k_dec_eff = _prefer_opponent(pd.DataFrame({k_col or "k": k_dec, **{c: df[c] for c in df.columns}}), "k_percent", k_col or "k")
    bb_dec_eff = _prefer_opponent(pd.DataFrame({bb_col or "bb": bb_dec, **{c: df[c] for c in df.columns}}), "bb_percent", bb_col or "bb")

    # Hits per AB: use column if present, else derive from counts
    h_ab_col = _first_existing(df, ALIASES["hits_per_ab"])
    if h_ab_col is not None:
        hits_per_ab = _normalize_rate_smart("hits_per_ab", df[h_ab_col])
    else:
        hits_per_ab = _derive_hits_per_ab(df, df["AB"])

    # HR per AB: use column if present, else try derive from counts else conservative from hits
    hr_ab_col = _first_existing(df, ALIASES["hr_per_ab"])
    if hr_ab_col is not None:
        hr_per_ab = _normalize_rate_smart("hr_per_ab", df[hr_ab_col])
    else:
        derived_hr = _derive_hr_per_ab(df, df["AB"])
        if derived_hr is not None:
            hr_per_ab = derived_hr
        else:
            hr_per_ab = _clip_series("hr_per_ab", 0.12 * hits_per_ab)

    # Projections (AB scaled)
    df["proj_hits"] = (hits_per_ab * df["AB"]).round(3)
    df["proj_hr"] = (hr_per_ab * df["AB"]).round(3)
    df["proj_avg"] = hits_per_ab.round(3)

    # Simple SLG proxy (per-AB total bases)
    xbh_share_col = _first_existing(df, ALIASES["xbh_share"])
    oneb_share_col = _first_existing(df, ALIASES["1b_share"])
    if xbh_share_col:
        xbh_share = _normalize_rate_smart("xbh_share", df[xbh_share_col])
    else:
        xbh_share = (hr_per_ab / hits_per_ab.replace(0, np.nan)).fillna(0).clip(0, 0.6) + 0.1
        xbh_share = _clip_series("xbh_share", xbh_share)

    if oneb_share_col:
        oneb_share = _normalize_rate_smart("1b_share", df[oneb_share_col])
    else:
        oneb_share = _clip_series("1b_share", 1.0 - xbh_share)

    singles_per_ab = (oneb_share * hits_per_ab).clip(0, 1)
    xbh_per_ab = (xbh_share * hits_per_ab).clip(0, 1)
    hr_per_ab = hr_per_ab.clip(0, xbh_per_ab)
    rem_xbh = (xbh_per_ab - hr_per_ab).clip(lower=0.0)
    doubles_per_ab = 0.65 * rem_xbh
    triples_per_ab = 0.10 * rem_xbh
    tb_per_ab = singles_per_ab*1 + doubles_per_ab*2 + triples_per_ab*3 + hr_per_ab*4
    df["proj_slg"] = tb_per_ab.round(3)

    df["k_percent_eff"] = _clip_series("k_percent", pd.to_numeric(k_dec_eff, errors="coerce")).round(4)
    df["bb_percent_eff"] = _clip_series("bb_percent", pd.to_numeric(bb_dec_eff, errors="coerce")).round(4)

    for c in ["proj_hits", "proj_hr", "proj_avg", "proj_slg"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).replace([np.inf, -np.inf], 0)

    return df
