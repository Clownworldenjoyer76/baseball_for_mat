
# scripts/project_batter_props.py
# Ensures required columns exist/are standardized before calling projection formulas.

import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections
import sys

FINAL_FILE = Path("data/end_chain/final/bat_today_final.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_projected.csv")

# Column normalization map: standard -> accepted aliases
ALIASES = {
    "PA": ["PA", "pa"],
    "BB%": ["BB%", "bb_percent", "bb_rate"],
    "K%": ["K%", "k_percent", "k_rate"],
    "H/AB": ["H/AB", "hits_per_ab", "AVG", "avg"],  # batting avg proxy
    # opponent context (optional but preferred)
    "opp_K%": ["opp_K%", "opp_k_percent", "opponent_k_percent"],
    "opp_BB%": ["opp_BB%", "opp_bb_percent", "opponent_bb_percent"],
    # optional HR rate per AB
    "HR/AB": ["HR/AB", "hr_per_ab", "hr_rate_ab"],
}

def _resolve_or_raise(df: pd.DataFrame, target: str) -> str:
    for cand in ALIASES.get(target, [target]):
        if cand in df.columns:
            return cand
        for col in df.columns:
            if col.lower() == cand.lower():
                return col
    raise ValueError(f"Missing required column for batters: {target} (accepted: {ALIASES.get(target)})")

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Required
    pa = _resolve_or_raise(df, "PA")
    bb = _resolve_or_raise(df, "BB%")
    k = _resolve_or_raise(df, "K%")
    hits_ab = _resolve_or_raise(df, "H/AB")
    # Optional; if absent, we don't create silent defaults (projection_formulas will handle preference)
    for opt in ["opp_K%", "opp_BB%", "HR/AB"]:
        try:
            _resolve_or_raise(df, opt)
        except ValueError:
            # keep absent; do not create placeholder to avoid silent defaulting
            pass
    # Return df unchanged (we operate by name inside formulas using aliases)
    return df

def main():
    try:
        df = pd.read_csv(FINAL_FILE)
    except Exception as e:
        print(f"Failed to read {FINAL_FILE}: {e}")
        sys.exit(1)

    df = _ensure_columns(df)

    df_proj = calculate_all_projections(df)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_proj.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote: {OUTPUT_FILE} ({len(df_proj)} rows)")

if __name__ == "__main__":
    main()
