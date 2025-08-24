from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


# ---------------- Paths ----------------
# Primary source now: data/Data/batters.csv (your attached file)
BATTERS_CANDIDATES: Iterable[Path] = [
    Path("data/Data/batters.csv"),
    Path("Data/batters.csv"),
    Path("batters.csv"),
]

# Where to write the projected per‑prop probabilities produced here
OUT_PATH = Path("data/_projections/batter_props_projected.csv")


# ---------------- Utilities ----------------
def _read_first_existing(paths: Iterable[Path]) -> Optional[pd.DataFrame]:
    for p in paths:
        if p.exists():
            try:
                return pd.read_csv(p)
            except Exception:
                pass
    return None


def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def _as_rate(series: pd.Series) -> pd.Series:
    """Accepts 0.123, 12.3, or '12.3%'. Returns fraction in [0,1]."""
    s = series.astype(str).str.strip().str.replace("%", "", regex=False)
    s = pd.to_numeric(s, errors="coerce")
    pct_mask = s.gt(1.0).fillna(False)
    s = s.where(~pct_mask, s / 100.0)
    return s.clip(lower=0.0)


def _poisson_tail_at_least_k(mu: pd.Series, k: int) -> pd.Series:
    mu = pd.to_numeric(mu, errors="coerce").fillna(0.0)
    if k <= 0:
        return pd.Series(1.0, index=mu.index, dtype=float)
    if k == 1:
        return 1.0 - np.exp(-mu)
    if k == 2:
        return 1.0 - np.exp(-mu) * (1.0 + mu)
    out = []
    for val in mu.values:
        if val <= 0:
            out.append(0.0)
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
    p = p.clip(0.0, 1.0 - 1e-12)
    return 1.0 - np.power((1.0 - p), n)


def _coalesce(df: pd.DataFrame, *cols: str, default=np.nan) -> pd.Series:
    out = pd.Series(default, index=df.index, dtype="float64")
    for c in cols:
        if c in df.columns:
            v = pd.to_numeric(df[c], errors="coerce")
            out = out.where(~out.isna(), v)
    return out


# ---------------- Core ----------------
def _load_inputs() -> pd.DataFrame:
    z = _read_first_existing(BATTERS_CANDIDATES)
    if z is None or z.empty:
        raise SystemExit(
            "❌ Could not find batters.csv in expected locations: "
            f"{', '.join(str(p) for p in BATTERS_CANDIDATES)}"
        )
    z = _std_cols(z)

    # ---- map the columns coming from your batters.csv ----
    # PA
    if "pa" in z.columns:
        pa = pd.to_numeric(z["pa"], errors="coerce")
    else:
        raise SystemExit("❌ batters.csv is missing required column: 'pa'")

    # AVG (batting average)
    avg = None
    for name in ["batting_avg", "avg", "ba", "xba"]:
        if name in z.columns:
            avg = pd.to_numeric(z[name], errors="coerce")
            break
    if avg is None:
        raise SystemExit("❌ batters.csv is missing an AVG column (e.g., 'batting_avg').")

    # ISO (isolated power)
    iso = None
    for name in ["isolated_power", "iso", "xiso"]:
        if name in z.columns:
            iso = pd.to_numeric(z[name], errors="coerce")
            break
    if iso is None:
        raise SystemExit("❌ batters.csv is missing an ISO column (e.g., 'isolated_power').")

    # HR rate — derive if needed from counts
    hr_rate = None
    for name in ["hr_rate", "proj_hr_rate", "xhr_rate"]:
        if name in z.columns:
            hr_rate = _as_rate(z[name])
            break
    if hr_rate is None:
        if "home_run" in z.columns:
            hr_rate = (pd.to_numeric(z["home_run"], errors="coerce") / pa).fillna(0.0)
        else:
            # final fallback: infer from ISO (same logic as before)
            hr_rate = (iso.fillna(0.120) * 0.3).clip(0.0, 0.15)

    # AB estimate (if AB present, use it; else derive from PA with BB/HBP/SF heuristics)
    if "ab" in z.columns:
        ab = pd.to_numeric(z["ab"], errors="coerce")
        ab = ab.fillna(0.88 * pa)
    else:
        bb_rate = _as_rate(_coalesce(z, "bb_rate", "bb_percent", "bb%")).fillna(0.08)
        ab = (1.0 - (bb_rate + 0.02 + 0.02)).clip(lower=0.5) * pa

    # Minimal identity columns if present
    keep_cols = [c for c in ["player_id", "name", "team", "game_id", "date"] if c in z.columns]
    out = pd.DataFrame(index=z.index)
    for c in keep_cols:
        out[c] = z[c]

    # If date is missing/blank, auto-fill today (YYYY-MM-DD)
    if "date" not in out.columns or out["date"].isna().all():
        out["date"] = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Attach the numeric inputs we’ll use downstream
    out["_pa"] = pa.fillna(0.0).clip(lower=0.0)
    out["_ab"] = ab.fillna(0.0).clip(lower=0.0)
    out["_avg"] = avg.fillna(0.0).clip(lower=0.0)
    out["_iso"] = iso.fillna(0.0).clip(lower=0.0)
    out["_hr_rate_pa"] = hr_rate.fillna(0.0).clip(lower=0.0)

    return out


def project_batter_props() -> pd.DataFrame:
    z = _load_inputs()

    pa = z["_pa"]
    ab = z["_ab"]
    p_hit_ab = z["_avg"].clip(0.0, 1.0 - 1e-12)

    # Convert HR per PA to per AB with the same simple AB/PA heuristic if needed
    # If ab==0, the division below is safe because we bound by minimum 1e-12.
    ab_over_pa = (ab / pa.replace(0, np.nan)).fillna(0.88).clip(lower=0.5)
    p_hr_ab = (z["_hr_rate_pa"] / ab_over_pa).clip(0.0, 1.0 - 1e-12)

    # Break out extra-base composition from ISO
    iso = z["_iso"].clip(lower=0.0)
    extra_from_hr = 3.0 * p_hr_ab
    rem_extra = (iso - extra_from_hr).clip(lower=0.0)
    p_double_ab = (rem_extra / 1.2).clip(lower=0.0)
    p_triple_ab = (0.1 * rem_extra / 1.2).clip(lower=0.0)

    p_single_ab = (p_hit_ab - (p_double_ab + p_triple_ab + p_hr_ab)).clip(lower=0.0)
    mask_bad = (p_single_ab + p_double_ab + p_triple_ab + p_hr_ab) > 1.0
    if mask_bad.any():
        scale = 0.999 / (p_single_ab + p_double_ab + p_triple_ab + p_hr_ab)
        p_single_ab *= scale

    # Probabilities
    mu_hits = ab * p_hit_ab
    prob_hits_over_1p5 = _poisson_tail_at_least_k(mu_hits, 2)

    mu_s = ab * p_single_ab
    mu_xb = ab * (p_double_ab + p_triple_ab + p_hr_ab)
    prob_tb_over_1p5 = 1.0 - np.exp(-(mu_s + mu_xb)) * (1.0 + mu_s)

    prob_hr_over_0p5 = _binom_one_or_more(pa, z["_hr_rate_pa"])

    # Build output
    out_cols = [c for c in ["player_id", "name", "team", "game_id", "date"] if c in z.columns]
    out = z[out_cols].copy()

    out["prob_hits_over_1p5"] = prob_hits_over_1p5.astype(float)
    out["prob_tb_over_1p5"] = prob_tb_over_1p5.astype(float)
    out["prob_hr_over_0p5"] = prob_hr_over_0p5.astype(float)

    # optional diagnostics
    out["proj_pa_used"] = pa
    out["proj_ab_used"] = ab
    out["proj_avg_used"] = p_hit_ab
    out["proj_iso_used"] = iso
    out["proj_hr_rate_pa_used"] = z["_hr_rate_pa"]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"✅ Wrote {len(out)} rows -> {OUT_PATH}")
    return out


if __name__ == "__main__":
    project_batter_props()
