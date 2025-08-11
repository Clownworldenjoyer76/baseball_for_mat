# scripts/project_batter_props.py — corrected so H/AB is optional and works with batting_avg

import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections
import sys

FINAL_FILE = Path("data/end_chain/final/bat_today_final.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_projected.csv")

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
    # Only PA is strictly required — all other rates can be derived from counts by projection_formulas
    _resolve(df, "PA", required=True)      # must exist
    _resolve(df, "BB%", required=False)
    _resolve(df, "K%", required=False)
    _resolve(df, "H/AB", required=False)
    _resolve(df, "HR/AB", required=False)
    _resolve(df, "opp_K%", required=False)
    _resolve(df, "opp_BB%", required=False)
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
