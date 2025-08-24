# scripts/project_batter_props.py
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional, Iterable

import numpy as np
import pandas as pd


# -------- Paths (robust fallbacks) --------
IN_CANDIDATES: Iterable[Path] = [
    Path("data/_projections/batter_props_z_expanded.csv"),
    Path("data/batter_props_z_expanded.csv"),
    Path("batter_props_z_expanded.csv"),
]

# Primary output your code has been using
OUT_PATH_PROJECTED = Path("data/_projections/batter_props_projected.csv")

# **NEW:** Also write to the path the next step has been expecting
OUT_PATH_PREP = Path("data/bets/prep/batter_props_final.csv")


# -------- Helpers --------
def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def _read_first_existing(paths: Iterable[Path]) -> Optional[pd.DataFrame]:
    for p in paths:
        if p.exists():
            try:
                return _std(pd.read_csv(p))
            except Exception:
                pass
    return None


def _to_float(s, default=np.nan) -> float:
    try:
        if isinstance(s, str):
            s = s.strip().replace("%", "")
        v = float(s)
        return v
    except Exception:
        return default


def _clip_nonneg(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    x = x.where(x >= 0, 0.0)
    return x


def _poisson_tail_at_least_k(mu: pd.Series, k: int) -> pd.Series:
    """P[X >= k] for X ~ Poisson(mu). Works elementwise for pandas Series."""
    mu = pd.to_numeric(mu, errors="coerce")
    if k <= 0:
        return pd.Series(np.ones(len(mu)), index=mu.index, dtype=float)
    if k == 1:
        return 1.0 - np.exp(-mu)
    if k == 2:
        return 1.0 - np.exp(-mu) * (1.0 + mu)
    out = []
    for val in mu.fillna(0.0).values:
        if val <= 0:
            out.append(0.0 if k > 0 else 1.0)
            continue
        s = 0.0
        term = 1.0  # mu^0 / 0!
        for i in range(k):
            if i > 0:
                term *= val / i
            s += term
        out.append(1.0 - math.exp(-val) * s)
    return pd.Series(out, index=mu.index, dtype=float)


def _binom_one_or_more(n: pd.Series, p: pd.Series) -> pd.Series:
    """P[X >= 1] for X ~ Binomial(n, p) = 1 - (1-p)^n."""
    n = pd.to_numeric(n, errors="coerce").fillna(0.0)
    p = pd.to_numeric(p, errors="coerce").fillna(0.0)
    p = p.where(p > 0, 0.0)
    p = p.where(p < 1, 1.0 - 1e-12)
    return 1.0 - np.power((1.0 - p), n)


def _as_rate(series: pd.Series) -> pd.Series:
    """Accepts values like 0.123, 12.3, or '12.3%'; returns fraction in [0,1]."""
    s = series.astype(str).str.strip().str.replace("%", "", regex=False)
    s = pd.to_numeric(s, errors="coerce")
    pct_mask = s.gt(1.0).fillna(False)
    s = s.where(~pct_mask, s / 100.0)
    s = s.clip(lower=0.0)
    return s


def _estimate_ab(pa: pd.Series, bb_rate: Optional[pd.Series]) -> pd.Series:
    """Rough AB estimate from PA and walk rate."""
    pa = pd.to_numeric(pa, errors="coerce").fillna(0.0)
    if bb_rate is None:
        return 0.88 * pa  # ~8% BB, 2% HBP, 2% SF
    r = _as_rate(bb_rate).fillna(0.08)
    return (1.0 - (r + 0.02 + 0.02)).clip(lower=0.5) * pa


def _coalesce(df: pd.DataFrame, *cols: str, default=np.nan) -> pd.Series:
    """Return first non-null column among given options."""
    out = pd.Series([default] * len(df), index=df.index, dtype="float64")
    for c in cols:
        if c in df.columns:
            v = pd.to_numeric(df[c], errors="coerce")
            out = out.where(~out.isna(), v)
    return out


def project_batter_props() -> pd.DataFrame:
    z = _read_first_existing(IN_CANDIDATES)
    if z is None or z.empty:
        raise SystemExit("❌ Could not find batter_props_z_expanded.csv in expected locations.")

    z = _std(z)

    # --- CORE INPUTS ---
    pa = _coalesce(z, "proj_pa", "pa").fillna(4.3)

    bb_rate = None
    for name in ["proj_bb_rate", "bb_rate", "bb_percent", "bb%"]:
        if name in z.columns:
            bb_rate = z[name]
            break

    ab = _estimate_ab(pa, bb_rate)

    p_hit_ab = _coalesce(z, "proj_avg", "proj_ba", "ba", "avg").clip(lower=0.0)
    p_hit_ab = p_hit_ab.where(p_hit_ab < 1, 1.0 - 1e-12)

    p_hr_pa = None
    for name in ["proj_hr_rate", "hr_rate", "xhr_rate"]:
        if name in z.columns:
            p_hr_pa = _as_rate(z[name])
            break
    if p_hr_pa is None:
        iso_tmp = _coalesce(z, "proj_iso", "iso").fillna(0.120)
        p_hr_pa = (iso_tmp * 0.3).clip(0.0, 0.15)

    bb_r = _as_rate(bb_rate) if bb_rate is not None else pd.Series([0.08] * len(z), index=z.index)
    ab_over_pa = (1.0 - (bb_r + 0.02 + 0.02)).clip(lower=0.5)
    p_hr_ab = (p_hr_pa / ab_over_pa).clip(lower=0.0)
    p_hr_ab = p_hr_ab.where(p_hr_ab < 1, 1.0 - 1e-12)

    iso = _coalesce(z, "proj_iso", "iso").fillna(0.120).clip(lower=0.0)
    extra_from_hr = 3.0 * p_hr_ab
    rem_extra = (iso - extra_from_hr).clip(lower=0.0)
    rate_2b_ab = (rem_extra / 1.2).clip(lower=0.0)
    rate_3b_ab = (0.1 * rem_extra / 1.2).clip(lower=0.0)

    p_double_ab = rate_2b_ab
    p_triple_ab = rate_3b_ab
    p_single_ab = (p_hit_ab - (p_double_ab + p_triple_ab + p_hr_ab)).clip(lower=0.0)
    mask_bad = (p_single_ab + p_double_ab + p_triple_ab + p_hr_ab) > 1.0
    if mask_bad.any():
        scale = 0.999 / (p_single_ab + p_double_ab + p_triple_ab + p_hr_ab)
        p_single_ab *= scale

    # --- PROBABILITIES ---
    mu_hits = ab * p_hit_ab
    prob_hits_over_1p5 = _poisson_tail_at_least_k(mu_hits, 2)

    mu_s = ab * p_single_ab
    mu_xb = ab * (p_double_ab + p_triple_ab + p_hr_ab)
    prob_tb_over_1p5 = 1.0 - np.exp(-(mu_s + mu_xb)) * (1.0 + mu_s)

    prob_hr_over_0p5 = _binom_one_or_more(pa, p_hr_pa)

    # -- Build output --
    cols_keep = [c for c in ["player_id", "name", "team", "game_id", "date"] if c in z.columns]
    out = pd.DataFrame(index=z.index)
    for c in cols_keep:
        out[c] = z[c]

    # If 'date' missing/blank -> fill with today's date (YYYY-MM-DD)
    if "date" not in out.columns or out["date"].isna().all():
        out["date"] = pd.Timestamp.now().date().strftime("%Y-%m-%d")

    out["prob_hits_over_1p5"] = prob_hits_over_1p5.astype(float)
    out["prob_tb_over_1p5"] = prob_tb_over_1p5.astype(float)
    out["prob_hr_over_0p5"] = prob_hr_over_0p5.astype(float)

    # Diagnostics (safe to keep)
    out["proj_pa_used"] = pa
    out["proj_ab_est"] = ab
    out["proj_avg_used"] = p_hit_ab
    out["proj_iso_used"] = iso
    out["proj_hr_rate_pa_used"] = p_hr_pa

    # Write BOTH locations so downstream steps see the expected file without changing workflows.
    OUT_PATH_PROJECTED.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH_PREP.parent.mkdir(parents=True, exist_ok=True)

    out.to_csv(OUT_PATH_PROJECTED, index=False)
    out.to_csv(OUT_PATH_PREP, index=False)

    print(f"✅ Wrote {len(out)} rows -> {OUT_PATH_PROJECTED}")
    print(f"✅ Wrote {len(out)} rows -> {OUT_PATH_PREP}  (for downstream steps)")

    return out


if __name__ == "__main__":
    project_batter_props()
