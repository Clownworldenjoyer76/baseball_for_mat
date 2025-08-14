# scripts/project_batter_props.py
# Corrected + adds upstream Poisson-based over probabilities for hits/HR/TB

from __future__ import annotations
import math
from pathlib import Path
import pandas as pd
from projection_formulas import calculate_all_projections

FINAL_FILE   = Path("data/end_chain/final/bat_today_final.csv")
OUTPUT_FILE  = Path("data/_projections/batter_props_projected.csv")

# Accept your actual headers
ALIASES = {
    "PA": ["pa", "PA"],
    "BB%": ["bb_percent", "BB%"],
    "K%": ["k_percent", "K%"],
    "H/AB": ["batting_avg", "H/AB", "hits_per_ab", "AVG", "avg"],
    "HR/AB": ["HR/AB", "hr_per_ab"],
    "opp_K%": ["opp_K%", "opp_k_percent", "opponent_k_percent"],
    "opp_BB%": ["opp_BB%", "opp_bb_percent", "opponent_bb_percent"],
}

def _resolve(df: pd.DataFrame, target: str, required: bool) -> str | None:
    cands = ALIASES.get(target, [target])
    for cand in cands:
        if cand in df.columns:
            return cand
        for col in df.columns:
            if col.lower() == cand.lower():
                return col
    if required:
        raise ValueError(f"Missing required column for batters: {target} (accepted: {cands})")
    return None

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Only PA is strictly required — the rest can be derived in projection_formulas
    _resolve(df, "PA", required=True)
    _resolve(df, "BB%", required=False)
    _resolve(df, "K%", required=False)
    _resolve(df, "H/AB", required=False)
    _resolve(df, "HR/AB", required=False)
    _resolve(df, "opp_K%", required=False)
    _resolve(df, "opp_BB%", required=False)
    return df

# ---- Probability helpers (no 0.98 cap) ----
def _poisson_cdf_le(k: int, lam: float) -> float:
    """P(X <= k) for Poisson(λ)."""
    k = int(k)
    if lam <= 0:
        return 1.0 if k >= 0 else 0.0
    term = math.exp(-lam)
    acc = term
    for i in range(1, k + 1):
        term *= lam / i
        acc += term
    return min(max(acc, 0.0), 1.0)

def _poisson_over_prob(lam: float, line_val: float) -> float:
    """
    P(X > line) where 'line' is fractional (e.g., 0.5, 1.5).
    threshold = floor(line) + 1 -> P(X >= threshold) = 1 - P(X <= threshold-1)
    """
    try:
        thr = int(math.floor(float(line_val))) + 1
    except Exception:
        return float("nan")
    if thr <= 0:
        return 1.0
    return float(min(max(1.0 - _poisson_cdf_le(thr - 1, float(lam)), 0.0), 1.0))

def main():
    # Load the day’s batter base and ensure expected columns exist
    df = pd.read_csv(FINAL_FILE)
    df = _ensure_columns(df)

    # Compute projections (adds AB, proj_hits, proj_hr, proj_slg, etc.)
    df_proj = calculate_all_projections(df)

    # Add upstream probabilities for common betting lines so downstream isn’t forced to guess
    # λ for hits is proj_hits; HR is proj_hr; TB approx λ ≈ proj_slg * AB
    for col in ("proj_hits", "proj_hr", "proj_slg", "AB"):
        if col not in df_proj.columns:
            df_proj[col] = float("nan")

    # Hits over 1.5 (i.e., 2+ hits)
    df_proj["prob_hits_over_1p5"] = [
        _poisson_over_prob(lam, 1.5) if pd.notna(lam) else float("nan")
        for lam in pd.to_numeric(df_proj["proj_hits"], errors="coerce")
    ]

    # HR over 0.5 (i.e., 1+ HR)
    df_proj["prob_hr_over_0p5"] = [
        _poisson_over_prob(lam, 0.5) if pd.notna(lam) else float("nan")
        for lam in pd.to_numeric(df_proj["proj_hr"], errors="coerce")
    ]

    # TB over 1.5 (≈ 2+ total bases) using λ ≈ proj_slg * AB
    tb_lambda = pd.to_numeric(df_proj["proj_slg"], errors="coerce") * pd.to_numeric(df_proj["AB"], errors="coerce")
    df_proj["prob_tb_over_1p5"] = [
        _poisson_over_prob(lam, 1.5) if pd.notna(lam) else float("nan")
        for lam in tb_lambda
    ]

    # Sanity: keep probabilities within [0,1] without arbitrary ceilings
    for c in ("prob_hits_over_1p5", "prob_hr_over_0p5", "prob_tb_over_1p5"):
        df_proj[c] = pd.to_numeric(df_proj[c], errors="coerce").clip(0, 1)

    # Persist projections + upstream probabilities
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_proj.to_csv(OUTPUT_FILE, index=False)
    print(
        f"Wrote: {OUTPUT_FILE} (rows={len(df_proj)}) | "
        f"cols={len(df_proj.columns)} incl: prob_hits_over_1p5, prob_hr_over_0p5, prob_tb_over_1p5"
    )

if __name__ == "__main__":
    main()
