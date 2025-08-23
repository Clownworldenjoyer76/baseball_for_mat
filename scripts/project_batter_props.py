# scripts/project_batter_props.py
from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Optional, Tuple, Dict

import numpy as np
import pandas as pd


IN_FILE  = Path("data/end_chain/final/bat_today_final.csv")
OUT_FILE = Path("data/_projections/batter_props_projected.csv")

# Hard caps to keep things realistic and avoid the 0.99 wall
LOW_CAP, HIGH_CAP = 0.05, 0.95

# Final columns we promise to produce
OUT_COLS = [
    "player_id", "name", "team",
    "prob_hits_over_1p5",
    "prob_tb_over_1p5",
    "prob_hr_over_0p5",
]


def _read_csv(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p)
    df.columns = df.columns.str.strip()
    return df


def _as_int64(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """Return the first column that exists (case-insensitive contains allowed)."""
    cols = list(df.columns)
    lower = {c.lower(): c for c in cols}
    # exact first
    for c in candidates:
        if c in cols:
            return c
        if c.lower() in lower:
            return lower[c.lower()]
    # substring fallback
    for c in candidates:
        for col in cols:
            if c.lower() in col.lower():
                return col
    return None


def _poisson_tail_at_least_k(mu: float, k: int) -> float:
    """P(X >= k) for X~Poisson(mu). For k=2: 1 - e^{-mu} * (1 + mu)."""
    if not (mu >= 0 and np.isfinite(mu)):
        return np.nan
    if k <= 0:
        return 1.0
    if k == 1:
        return 1.0 - math.exp(-mu)
    if k == 2:
        return 1.0 - math.exp(-mu) * (1.0 + mu)
    # generic sum (rarely used for k>2 here)
    acc = 0.0
    term = math.exp(-mu)  # P(0)
    acc += term
    for n in range(1, k):
        term *= mu / n
        acc += term
    return max(0.0, min(1.0, 1.0 - acc))


def _rank_to_prob(x: pd.Series, lo: float = 0.40, hi: float = 0.60) -> pd.Series:
    """Monotone calibration when we only have a score/rank."""
    s = pd.to_numeric(x, errors="coerce")
    r = s.rank(method="average", na_option="keep")
    frac = (r - r.min()) / (r.max() - r.min()) if r.notna().sum() > 1 else pd.Series(np.nan, index=x.index)
    return lo + frac * (hi - lo)


def _clip_prob(p: pd.Series, lo: float = LOW_CAP, hi: float = HIGH_CAP) -> pd.Series:
    return pd.to_numeric(p, errors="coerce").clip(lo, hi)


def _anti_flatten(p: pd.Series, target_lo=0.40, target_hi=0.60) -> pd.Series:
    """If the distribution is flat (very low std or few uniques), rescale by rank."""
    s = pd.to_numeric(p, errors="coerce")
    s_valid = s.dropna()
    if s_valid.nunique() <= 5 or (s_valid.std() < 0.03 and s_valid.mean() > 0 and s_valid.mean() < 1):
        return _rank_to_prob(s, target_lo, target_hi)
    return s


def _derive_probs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize a few key fields
    if "player_id" in df.columns:
        df["player_id"] = _as_int64(df["player_id"])
    if "team" in df.columns:
        df["team"] = df["team"].astype(str)

    # --- Attempt to find useful numeric predictors ---
    # Hits expected mean (per game)
    hits_mu_col = _pick_col(df, [
        "hits_mu", "exp_hits", "expected_hits", "proj_hits", "x_hits", "mean_hits"
    ])
    # Total bases expected mean — imperfect but Poisson(μ_tb) is a reasonable monotone heuristic
    tb_mu_col = _pick_col(df, [
        "tb_mu", "exp_tb", "expected_tb", "proj_tb", "x_tb", "mean_tb", "total_bases_mu"
    ])
    # Home run probability or rate
    # Prefer a calibrated per-game probability if present; else use a rate proxy (HR per PA or per AB)
    hr_prob_col = _pick_col(df, [
        "hr_prob", "prob_hr", "p_hr", "home_run_prob"
    ])
    hr_rate_proxy_col = hr_prob_col or _pick_col(df, [
        "hr_rate", "x_hr", "proj_hr", "hr_per_game", "hr/pa", "hr_pa", "hr_ab"
    ])

    # Fallback score if none of the above are present
    generic_score_col = _pick_col(df, [
        "value", "score", "model_score", "raw", "rank_score", "composite", "p_over"
    ])

    # --- Compute probabilities ---

    # Hits OVER 1.5
    if hits_mu_col:
        mu = pd.to_numeric(df[hits_mu_col], errors="coerce")
        p_hits = mu.apply(lambda m: _poisson_tail_at_least_k(m, 2))
    else:
        # fallback to rank calibration on anything we have
        src = None
        for c in [generic_score_col, tb_mu_col, hr_rate_proxy_col]:
            if c:
                src = c; break
        p_hits = _rank_to_prob(df[src]) if src else pd.Series(np.nan, index=df.index)
    p_hits = _clip_prob(_anti_flatten(p_hits))

    # TB OVER 1.5
    if tb_mu_col:
        mu_tb = pd.to_numeric(df[tb_mu_col], errors="coerce")
        p_tb = mu_tb.apply(lambda m: _poisson_tail_at_least_k(m, 2))
    else:
        src = None
        for c in [generic_score_col, hits_mu_col, hr_rate_proxy_col]:
            if c:
                src = c; break
        p_tb = _rank_to_prob(df[src], lo=0.42, hi=0.62) if src else pd.Series(np.nan, index=df.index)
    p_tb = _clip_prob(_anti_flatten(p_tb))

    # HR OVER 0.5
    if hr_prob_col:
        p_hr = pd.to_numeric(df[hr_prob_col], errors="coerce")
    elif hr_rate_proxy_col:
        rate = pd.to_numeric(df[hr_rate_proxy_col], errors="coerce")
        # If it's a per‑PA rate and we don't know PA, map monotonically with a gentle logistic
        # to keep plausible ranges; this avoids huge inflation.
        # p = sigmoid(a + b * rate); choose b small.
        a, b = -3.0, 18.0  # median ~0.047 when rate≈0.17 (tunable but conservative)
        p_hr = 1.0 / (1.0 + np.exp(-(a + b * rate)))
    else:
        p_hr = _rank_to_prob(df[generic_score_col], lo=0.05, hi=0.20) if generic_score_col else pd.Series(np.nan, index=df.index)
    p_hr = _clip_prob(_anti_flatten(p_hr, target_lo=0.06, target_hi=0.22), lo=0.02, hi=0.45)

    out = pd.DataFrame({
        "player_id": df.get("player_id"),
        "name": df.get("name"),
        "team": df.get("team"),
        "prob_hits_over_1p5": p_hits,
        "prob_tb_over_1p5": p_tb,
        "prob_hr_over_0p5": p_hr,
    })
    return out


def _validate_print(df: pd.DataFrame) -> None:
    def s(col):
        x = pd.to_numeric(df[col], errors="coerce")
        return {
            "min": round(float(x.min()), 4),
            "p25": round(float(x.quantile(0.25)), 4),
            "mean": round(float(x.mean()), 4),
            "p75": round(float(x.quantile(0.75)), 4),
            "max": round(float(x.max()), 4),
            "n_unique": int(x.nunique()),
        }

    print("\n=== Projection probability sanity check ===")
    for c in ["prob_hits_over_1p5", "prob_tb_over_1p5", "prob_hr_over_0p5"]:
        if c in df.columns and df[c].notna().any():
            print(f"{c}: {s(c)}")
        else:
            print(f"{c}: MISSING or all NA")


def main():
    if not IN_FILE.exists():
        raise SystemExit(f"Input not found: {IN_FILE}")

    raw = _read_csv(IN_FILE)
    proj = _derive_probs(raw)

    # Final clip & tidy
    for c in ["prob_hits_over_1p5", "prob_tb_over_1p5", "prob_hr_over_0p5"]:
        proj[c] = _clip_prob(proj[c])

    # Ensure folder and write
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    proj[OUT_COLS].to_csv(OUT_FILE, index=False)

    _validate_print(proj)
    print(f"\n✅ Wrote projections to {OUT_FILE} ({len(proj)} rows)")


if __name__ == "__main__":
    main()
