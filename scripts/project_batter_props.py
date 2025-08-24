# scripts/project_batter_props.py
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional, Iterable, Tuple

import numpy as np
import pandas as pd


# ---------------- Paths ----------------
IN_CANDIDATES: Iterable[Path] = [
    Path("data/_projections/batter_props_z_expanded.csv"),
    Path("data/batter_props_z_expanded.csv"),
    Path("batter_props_z_expanded.csv"),
]

# where we can backfill missing inputs (your “batters.csv”)
BATTERS_CANDIDATES: Iterable[Path] = [
    Path("data/Data/batters.csv"),   # your repo’s path
    Path("data/data/batters.csv"),
    Path("data/batters.csv"),
    Path("batters.csv"),
]

OUT_PATH = Path("data/_projections/batter_props_projected.csv")


# ---------------- Helpers ----------------
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
    """Accepts values like 0.123, 12.3, or '12.3%'; returns fraction in [0,1]."""
    s = series.astype(str).str.strip().str.replace("%", "", regex=False)
    s = pd.to_numeric(s, errors="coerce")
    pct_mask = s.gt(1.0).fillna(False)
    s = s.where(~pct_mask, s / 100.0)
    return s.clip(lower=0.0)


def _coalesce(df: pd.DataFrame, *cols: str, default=np.nan) -> pd.Series:
    out = pd.Series([default] * len(df), index=df.index, dtype="float64")
    for c in cols:
        if c in df.columns:
            v = pd.to_numeric(df[c], errors="coerce")
            out = out.where(~out.isna(), v)
    return out


def _estimate_ab(pa: pd.Series, bb_rate: Optional[pd.Series]) -> pd.Series:
    pa = pd.to_numeric(pa, errors="coerce").fillna(0.0)
    if bb_rate is None:
        return 0.88 * pa  # generic: ~8% BB, 2% HBP, 2% SF
    r = _as_rate(bb_rate).fillna(0.08)
    return (1.0 - (r + 0.02 + 0.02)).clip(lower=0.5) * pa


def _poisson_tail_at_least_k(mu: pd.Series, k: int) -> pd.Series:
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
        term = 1.0
        for i in range(k):
            if i > 0:
                term *= val / i
            s += term
        out.append(1.0 - math.exp(-val) * s)
    return pd.Series(out, index=mu.index, dtype=float)


def _binom_one_or_more(n: pd.Series, p: pd.Series) -> pd.Series:
    n = pd.to_numeric(n, errors="coerce").fillna(0.0)
    p = _as_rate(p).fillna(0.0)
    p = p.where(p > 0, 0.0).where(p < 1, 1.0 - 1e-12)
    return 1.0 - np.power((1.0 - p), n)


def _load_inputs() -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    z = _read_first_existing(IN_CANDIDATES)
    if z is None or z.empty:
        raise SystemExit("❌ Could not find batter_props_z_expanded.csv in expected locations.")
    bats = _read_first_existing(BATTERS_CANDIDATES)
    return _std(z), (None if bats is None or bats.empty else _std(bats))


def _backfill_from_batters(z: pd.DataFrame, bats: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing PA / AVG / ISO / HR-rate in `z` using `data/Data/batters.csv`.
    Joins on `player_id`. Does not overwrite existing non-null values.
    """
    if "player_id" not in z.columns or "player_id" not in bats.columns:
        return z

    keep = ["player_id", "pa", "ab", "avg", "iso", "hr"]
    b = bats[[c for c in keep if c in bats.columns]].copy()

    # derive safe helpers
    if "pa" in b.columns and "hr" in b.columns:
        b["hr_rate_pa"] = (pd.to_numeric(b["hr"], errors="coerce") /
                           pd.to_numeric(b["pa"], errors="coerce")).replace([np.inf, -np.inf], np.nan)
    else:
        b["hr_rate_pa"] = np.nan

    # left-join
    z = z.merge(b, on="player_id", how="left", suffixes=("", "_bats"))

    # only fill where z is NA
    def fill(col_z_list, col_b):
        # fill first available z-column with b[col_b] if NA
        for col_z in col_z_list:
            if col_z in z.columns:
                z[col_z] = z[col_z].where(z[col_z].notna(), z[col_b])
                return
        # if none of the z-columns exist, create the first name
        z[col_z_list[0]] = z[col_b]

    # PA
    fill(["proj_pa", "pa"], "pa")
    # AVG / BA
    fill(["proj_avg", "proj_ba", "avg", "ba"], "avg")
    # ISO
    fill(["proj_iso", "iso"], "iso")
    # HR rate per PA
    if "proj_hr_rate" not in z.columns:
        z["proj_hr_rate"] = np.nan
    z["proj_hr_rate"] = z["proj_hr_rate"].where(z["proj_hr_rate"].notna(), z["hr_rate_pa"])

    # clean
    return z


def project_batter_props() -> pd.DataFrame:
    z, bats = _load_inputs()

    # backfill from batters.csv if available
    if bats is not None:
        z = _backfill_from_batters(z, bats)

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
        iso_for_fallback = _coalesce(z, "proj_iso", "iso").fillna(0.120)
        p_hr_pa = (iso_for_fallback * 0.3).clip(0.0, 0.15)

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

    mu_hits = ab * p_hit_ab
    prob_hits_over_1p5 = _poisson_tail_at_least_k(mu_hits, 2)

    mu_s = ab * p_single_ab
    mu_xb = ab * (p_double_ab + p_triple_ab + p_hr_ab)
    prob_tb_over_1p5 = 1.0 - np.exp(-(mu_s + mu_xb)) * (1.0 + mu_s)

    prob_hr_over_0p5 = _binom_one_or_more(pa, p_hr_pa)

    cols_keep = [c for c in ["player_id", "name", "team", "game_id", "date"] if c in z.columns]
    out = pd.DataFrame(index=z.index)
    for c in cols_keep:
        out[c] = z[c]

    out["prob_hits_over_1p5"] = prob_hits_over_1p5.astype(float)
    out["prob_tb_over_1p5"] = prob_tb_over_1p5.astype(float)
    out["prob_hr_over_0p5"] = prob_hr_over_0p5.astype(float)

    # diagnostics
    out["proj_pa_used"] = pa
    out["proj_ab_est"] = ab
    out["proj_avg_used"] = p_hit_ab
    out["proj_iso_used"] = iso
    out["proj_hr_rate_pa_used"] = p_hr_pa

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PATH, index=False)

    # lightweight visibility on how much we backfilled
    def _missing_rate(s: pd.Series) -> float:
        return float(pd.isna(s).mean()) if len(s) else 0.0

    print("✅ Wrote", len(out), "rows ->", OUT_PATH)
    print(
        "Backfill summary (share missing BEFORE backfill):",
        {
            "pa": _missing_rate(_coalesce(z, "proj_pa", "pa")),
            "avg": _missing_rate(_coalesce(z, "proj_avg", "proj_ba", "ba", "avg")),
            "iso": _missing_rate(_coalesce(z, "proj_iso", "iso")),
            "hr_rate": _missing_rate(_coalesce(z, "proj_hr_rate", "hr_rate")),
        },
    )
    return out


if __name__ == "__main__":
    project_batter_props()
