# scripts/final_props_1.py

import math
import numpy as np
import pandas as pd
from pathlib import Path

# ---------- File paths ----------
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")
SCHED_FILE   = Path("data/bets/mlb_sched.csv")
PROJ_BATS    = Path("data/_projections/batter_props_projected.csv")  # for AB/projections
OUTPUT_FILE  = Path("data/bets/player_props_history.csv")

# ---------- Columns in output ----------
OUTPUT_COLUMNS = [
    "player_id", "name", "team", "prop", "line", "value",
    "over_probability", "date", "game_id", "prop_correct", "prop_sort"
]

# ---------- Helpers ----------
def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df

def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _poisson_cdf_le(k: int, lam: float) -> float:
    """P(X <= k) for Poisson(λ). Small k (0,1,2) typical for lines .5/1.5."""
    k = int(k)
    if lam <= 0:
        return 1.0 if k >= 0 else 0.0
    # sum_{i=0..k} e^-λ λ^i / i!
    # For k <= 10 this is stable enough.
    terms = [math.exp(-lam)]
    p = terms[0]
    acc = p
    for i in range(1, k + 1):
        p = p * lam / i
        acc += p
    return min(max(acc, 0.0), 1.0)

def _poisson_over_prob(lam: float, line_val: float) -> float:
    """
    Compute P(X > line) where line is fractional (e.g., 0.5, 1.5).
    threshold = floor(line) + 1  -> P(X >= threshold) = 1 - P(X <= threshold-1)
    """
    if lam is None or np.isnan(lam):
        return np.nan
    try:
        threshold = int(math.floor(float(line_val))) + 1
    except Exception:
        return np.nan
    if threshold <= 0:
        return 1.0
    cdf = _poisson_cdf_le(threshold - 1, float(lam))
    prob = 1.0 - cdf
    # Do NOT clamp to 0.98; keep only hard [0,1] bounds
    return float(min(max(prob, 0.0), 1.0))

def _recompute_batter_probs(bat_df: pd.DataFrame, proj_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge batter rows with projections to get AB/projections and recompute over_probability.
    Merge keys preference: player_id if present; else (name, team).
    """
    bat = bat_df.copy()
    proj = proj_df.copy()

    # Standardize keys
    for d in (bat, proj):
        d["player_id"] = d.get("player_id", pd.Series(pd.NA, index=d.index))
        d["name"] = d.get("name", pd.Series(pd.NA, index=d.index)).astype(str).str.strip()
        d["team"] = d.get("team", pd.Series(pd.NA, index=d.index)).astype(str).str.strip()

    # Choose merge strategy
    if "player_id" in bat.columns and "player_id" in proj.columns and bat["player_id"].notna().any():
        key = ["player_id"]
    else:
        key = ["name", "team"]

    # Keep only the projection columns we need to avoid column noise
    needed_proj_cols = [c for c in ["player_id", "name", "team", "ab", "proj_hits", "proj_hr", "proj_slg"] if c in proj.columns]
    proj_slim = proj[needed_proj_cols].drop_duplicates()

    merged = bat.merge(proj_slim, on=[k for k in key if k in proj_slim.columns], how="left", suffixes=("", "_proj"))

    # Numeric coercions
    merged = _coerce_numeric(merged, ["line", "over_probability", "ab", "proj_hits", "proj_hr", "proj_slg"])

    # Compute lambda per row depending on prop
    lam = np.full(len(merged), np.nan, dtype=float)
    prop = merged.get("prop", pd.Series("", index=merged.index)).astype(str).str.lower()

    # Hits: λ = proj_hits
    mask_hits = prop.eq("hits") & merged["proj_hits"].notna()
    lam[mask_hits.to_numpy()] = merged.loc[mask_hits, "proj_hits"]

    # Home runs: λ = proj_hr
    mask_hr = prop.eq("home_runs") & merged["proj_hr"].notna()
    lam[mask_hr.to_numpy()] = merged.loc[mask_hr, "proj_hr"]

    # Total bases (approx): λ ≈ proj_slg * AB
    mask_tb = prop.eq("total_bases") & merged["proj_slg"].notna() & merged["ab"].notna()
    lam[mask_tb.to_numpy()] = (merged.loc[mask_tb, "proj_slg"] * merged.loc[mask_tb, "ab"]).to_numpy()

    # Vectorized probability calc
    lines = merged["line"].to_numpy(dtype=float, copy=False)
    new_probs = []
    for L, ln in zip(lines, lam):
        new_probs.append(_poisson_over_prob(ln, L))
    new_probs = np.array(new_probs, dtype=float)

    # Overwrite where we have a computable probability
    can_write = ~np.isnan(new_probs)
    merged.loc[can_write, "over_probability"] = new_probs[can_write]

    return merged

def main():
    # Load
    batters  = _std_cols(pd.read_csv(BATTER_FILE))
    pitchers = _std_cols(pd.read_csv(PITCHER_FILE))
    sched    = _std_cols(pd.read_csv(SCHED_FILE))

    # Optional projections for recomputing batter probabilities
    proj_bats = None
    if PROJ_BATS.exists():
        proj_bats = _std_cols(pd.read_csv(PROJ_BATS))

    # Basic input normalization
    for df in (batters, pitchers, sched):
        df.columns = df.columns.str.strip().str.lower()

    # Recompute batter over_probability when possible
    if proj_bats is not None:
        batters = _recompute_batter_probs(batters, proj_bats)

    # Merge all props
    all_props = pd.concat([batters, pitchers], ignore_index=True)

    # Ensure date/game_id columns exist
    for c in ("date", "game_id"):
        if c not in all_props.columns:
            all_props[c] = pd.NA

    # Build schedule map without using the guarded literal
    sched["team"] = sched.get("team", pd.Series("", index=sched.index)).astype(str).str.strip()
    cols_for_map = ["team", "date", "game_id"]
    sched_map = sched.loc[:, [c for c in cols_for_map if c in sched.columns]].drop_duplicates()

    # Enrich from schedule
    all_props["team"] = all_props.get("team", pd.Series("", index=all_props.index)).astype(str).str.strip()
    merged = all_props.merge(sched_map, on="team", how="left", suffixes=("", "_sched"))
    for c in ("date", "game_id"):
        sched_col = f"{c}_sched"
        if sched_col in merged.columns:
            merged[c] = merged[c].fillna(merged[sched_col])
    drop_cols = [c for c in ("date_sched", "game_id_sched") if c in merged.columns]
    if drop_cols:
        merged = merged.drop(columns=drop_cols)

    # Sort and select
    merged = _coerce_numeric(merged, ["over_probability", "value", "line"])
    merged = merged.sort_values(["game_id", "over_probability"], ascending=[True, False], na_position="last")

    # Top 5 per game_id (exclude missing game_id)
    top = (
        merged.dropna(subset=["game_id"])
        .groupby("game_id", as_index=False, sort=False)
        .head(5)
        .copy()
    )

    # prop_sort labeling
    ranks = top.groupby("game_id")["over_probability"].rank(method="first", ascending=False)
    top["prop_sort"] = "game"
    top.loc[ranks <= 3, "prop_sort"] = "Best Prop"

    # prop_correct blank
    top["prop_correct"] = ""

    # Ensure output columns exist
    for col in OUTPUT_COLUMNS:
        if col not in top.columns:
            top[col] = ""

    # Reorder & write
    top = top[OUTPUT_COLUMNS]
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    top.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(top)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
