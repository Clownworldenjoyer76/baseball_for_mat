#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import poisson

INPUT_FILE  = Path("data/bets/prep/batter_props_bets.csv")
OUTPUT_FILE = Path("data/bets/batter_props_final.csv")


# --- Normalize season totals to per-game ---
def normalize_per_game(df: pd.DataFrame) -> pd.DataFrame:
    # Prefer games played, else ABs, else fallback
    if "games_played" in df.columns:
        g = df["games_played"].replace(0, 1)
    elif "ab" in df.columns:  # use at-bats as proxy (~4 AB per game)
        g = (df["ab"] / 4).replace(0, 1)
    else:
        g = 150  # fallback if no denominator available

    if "proj_hits" in df.columns:
        df["proj_hits"] = df["proj_hits"] / g
    if "proj_hr" in df.columns:
        df["proj_hr"] = df["proj_hr"] / g
    if "b_total_bases" in df.columns:
        df["b_total_bases"] = df["b_total_bases"] / g

    return df


def prob_over_poisson(mean: float, line: float) -> float:
    """Probability X > line given Poisson(mean)."""
    if mean <= 0 or np.isnan(mean):
        return 0.0
    k = int(np.floor(line))
    return 1.0 - poisson.cdf(k, mean)


def main():
    if not INPUT_FILE.exists():
        raise SystemExit(f"❌ Missing {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    # --- normalize projections ---
    df = normalize_per_game(df)

    # --- probabilities ---
    if "proj_hits" in df.columns:
        df["prob_hits_over_1p5"] = df["proj_hits"].apply(
            lambda x: prob_over_poisson(x, 1.5)
        )

    if "proj_hr" in df.columns:
        df["prob_hr_over_0p5"] = df["proj_hr"].apply(
            lambda x: prob_over_poisson(x, 0.5)
        )

    if "b_total_bases" in df.columns:
        df["prob_tb_over_1p5"] = df["b_total_bases"].apply(
            lambda x: prob_over_poisson(x, 1.5)
        )

    # --- output ---
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote: {OUTPUT_FILE} (rows={len(df)})")


if __name__ == "__main__":
    main()
