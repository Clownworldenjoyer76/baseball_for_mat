# scripts/project_batter_props.py
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional, Iterable, Dict, List, Tuple

import numpy as np
import pandas as pd


# -------- Paths (robust fallbacks) --------
IN_CANDIDATES: Iterable[Path] = [
    Path("data/_projections/batter_props_z_expanded.csv"),
    Path("data/batter_props_z_expanded.csv"),
    Path("batter_props_z_expanded.csv"),
]
OUT_PATH = Path("data/_projections/batter_props_projected.csv")


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


def _as_rate(series: pd.Series) -> pd.Series:
    """Accept 0.123, 12.3, or '12.3%'; return fraction in [0,1]."""
    s = series.astype(str).str.strip().str.replace("%", "", regex=False)
    s = pd.to_numeric(s, errors="coerce")
    pct_mask = s.gt(1.0).fillna(False)
    s = s.where(~pct_mask, s / 100.0)
    s = s.clip(lower=0.0)
    return s


def _coalesce(
    df: pd.DataFrame, candidates: List[str], *, rate=False, numeric=True
) -> Tuple[pd.Series, pd.Series]:
    """
    Return first non-null among candidate columns.
    Also return a boolean mask indicating rows that came from a real column (not default).
    """
    base = pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    source_mask = pd.Series(False, index=df.index)
    for c in candidates:
        if c in df.columns:
            v = df[c]
            if rate:
                v = _as_rate(v)
            elif numeric:
                v = pd.to_numeric(v, errors="coerce")
            m = base.isna() & v.notna()
            base.loc[m] = v.loc[m]
            source_mask |= m
    return base, source_mask


def _estimate_ab(pa: pd.Series, bb_rate: Optional[pd.Series]) -> pd.Series:
    """Rough AB estimate from PA and walk rate."""
    pa = pd.to_numeric(pa, errors="coerce").fillna(0.0)
    if bb_rate is None:
        return 0.88 * pa  # generic: 8% BB, 2% HBP, 2% SF
    r = _as_rate(bb_rate).fillna(0.08)
    return (1.0 - (r + 0.02 + 0.02)).clip(lower=0.5) * pa


def _poisson_tail_at_least_k(mu: pd.Series, k: int) -> pd.Series:
    mu = pd.to_numeric(mu, errors="coerce").fillna(0.0)
    if k <= 0:
        return pd.Series(np.ones(len(mu)), index=mu.index, dtype=float)
    if k == 1:
        return 1.0 - np.exp(-mu)
    if k == 2:
        return 1.0 - np.exp(-mu) * (1.0 + mu)
    out = []
    for val in mu.values:
        if val <= 0:
            out.append(0.0 if k > 0 else 1.0)
            continue
        s = 0.0
        term = 1.0
        for i in range(k):
            if i > 0:
                term *= val / i
            s += term
        out.append(1.0 - math.exp(-val) * s)
    return pd.Series(out, index=mu.index, dtype=float)


def _binom_one_or_more(n: pd.Series, p: pd.Series) -> pd.Series:
    n = pd.to_numeric(n, errors="coerce").fillna(0.0)
    p = pd.to_numeric(p, errors="coerce").fillna(0.0)
    p = p.clip(lower=0.0, upper=1.0 - 1e-12)
    return 1.0 - np.power((1.0 - p), n)


# -------- Main projection --------
def project_batter_props() -> pd.DataFrame:
    z = _read_first_existing(IN_CANDIDATES)
    if z is None or z.empty:
        raise SystemExit("❌ Could not find batter_props_z_expanded.csv in expected locations.")

    z = _std(z)

    # --- Try many reasonable column names from typical prep files ---
    pa_cols = ["proj_pa", "pa", "plate_appearances", "xPA", "est_pa"]
    avg_cols = ["proj_avg", "proj_ba", "ba", "avg", "xBA", "batting_avg"]
    iso_cols = ["proj_iso", "iso", "xISO"]
    bb_cols = ["proj_bb_rate", "bb_rate", "bb_percent", "bb%", "walk_rate"]
    hr_rate_cols = ["proj_hr_rate", "hr_rate", "xhr_rate", "hr_per_pa", "hr%"]

    # Coalesce inputs
    pa, pa_from_data = _coalesce(z, pa_cols, numeric=True)
    ba, ba_from_data = _coalesce(z, avg_cols, numeric=True)
    iso, iso_from_data = _coalesce(z, iso_cols, numeric=True)
    bb_rate, bb_from_data = _coalesce(z, bb_cols, rate=True)
    hr_pa, hr_from_data = _coalesce(z, hr_rate_cols, rate=True)

    # Apply defaults where missing
    defaults_used: Dict[str, float] = {}

    if pa.isna().any():
        defaults_used["PA"] = pa.isna().sum()
        pa = pa.fillna(4.3)

    if ba.isna().any():
        defaults_used["AVG"] = ba.isna().sum()
        ba = ba.fillna(0.245)

    if iso.isna().any():
        defaults_used["ISO"] = iso.isna().sum()
        iso = iso.fillna(0.120)

    if hr_pa.isna().any():
        defaults_used["HR/PA (from ISO*0.3)"] = hr_pa.isna().sum()
        hr_pa = (iso * 0.3).clip(0.0, 0.15)

    # Derived
    ab = _estimate_ab(pa, bb_rate if bb_from_data.any() else None)
    bb_r = _as_rate(bb_rate) if bb_from_data.any() else pd.Series(0.08, index=z.index)
    ab_over_pa = (1.0 - (bb_r + 0.02 + 0.02)).clip(lower=0.5)
    hr_ab = (hr_pa / ab_over_pa).clip(0.0, 1.0 - 1e-12)

    # Split hits composition using AVG + ISO
    extra_from_hr = 3.0 * hr_ab
    rem_extra = (iso - extra_from_hr).clip(lower=0.0)
    rate_2b_ab = (rem_extra / 1.2).clip(lower=0.0)
    rate_3b_ab = (0.1 * rem_extra / 1.2).clip(lower=0.0)
    p_double_ab = rate_2b_ab
    p_triple_ab = rate_3b_ab
    p_hit_ab = ba.clip(0.0, 1.0 - 1e-12)
    p_single_ab = (p_hit_ab - (p_double_ab + p_triple_ab + hr_ab)).clip(lower=0.0)

    # Guard against sums > 1
    total_p = p_single_ab + p_double_ab + p_triple_ab + hr_ab
    mask_bad = total_p > 1.0
    if mask_bad.any():
        scale = 0.999 / total_p[mask_bad]
        p_single_ab.loc[mask_bad] *= scale

    # Probabilities
    mu_hits = ab * p_hit_ab
    prob_hits_over_1p5 = _poisson_tail_at_least_k(mu_hits, 2)

    mu_s = ab * p_single_ab
    mu_xb = ab * (p_double_ab + p_triple_ab + hr_ab)
    prob_tb_over_1p5 = 1.0 - np.exp(-(mu_s + mu_xb)) * (1.0 + mu_s)

    prob_hr_over_0p5 = _binom_one_or_more(pa, hr_pa)

    # Build output
    cols_keep = [c for c in ["player_id", "name", "team", "game_id", "date"] if c in z.columns]
    out = pd.DataFrame(index=z.index)
    for c in cols_keep:
        out[c] = z[c]

    out["prob_hits_over_1p5"] = prob_hits_over_1p5.astype(float)
    out["prob_tb_over_1p5"] = prob_tb_over_1p5.astype(float)
    out["prob_hr_over_0p5"] = prob_hr_over_0p5.astype(float)

    # Minimal diagnostics to catch “flat” issues upstream
    n = len(out)
    def _pct(x: int) -> str:
        return f"{(100.0 * x / max(n,1)):.1f}%"

    used_defaults_lines = []
    for k, cnt in defaults_used.items():
        used_defaults_lines.append(f"- {k}: {cnt} rows ({_pct(int(cnt))})")

    # If too many defaults, fail with an actionable message
    high_default_share = any(int(cnt) > 0.6 * n for cnt in defaults_used.values())
    if high_default_share:
        lines = "\n".join(used_defaults_lines) if used_defaults_lines else "(none)"
        raise SystemExit(
            "❌ Too many rows used fallback defaults — projections will look flat.\n"
            "What to do: ensure the input file includes real columns for PA/AVG/ISO/HR rate.\n"
            "Defaults used:\n" + lines
        )

    # Save
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"✅ Wrote {len(out)} rows -> {OUT_PATH}")

    # Brief summary to stdout (helps the validator step)
    if used_defaults_lines:
        print("ℹ️  Defaults used:\n" + "\n".join(used_defaults_lines))

    print(
        "ℹ️  Probability distribution snapshot (HR>=1): "
        f"min={out['prob_hr_over_0p5'].min():.3f}, "
        f"25%={out['prob_hr_over_0p5'].quantile(0.25):.3f}, "
        f"50%={out['prob_hr_over_0p5'].quantile(0.50):.3f}, "
        f"75%={out['prob_hr_over_0p5'].quantile(0.75):.3f}, "
        f"max={out['prob_hr_over_0p5'].max():.3f}"
    )
    return out


if __name__ == "__main__":
    project_batter_props()
