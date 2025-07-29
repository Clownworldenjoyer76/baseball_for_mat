# project_batter_props.py
import pandas as pd
from pathlib import Path

# --- CHANGE THESE IMPORTS ---
# From: from .utils import load_csv
#       from .data_preprocessing import merge_with_pitcher_data, apply_batter_fallback_stats
#       from .projection_formulas import calculate_all_projections
# To:   from utils import load_csv
#       from data_preprocessing import merge_with_pitcher_data, apply_batter_fallback_stats
#       from projection_formulas import calculate_all_projections
from utils import load_csv
from data_preprocessing import merge_with_pitcher_data, apply_batter_fallback_stats
from projection_formulas import calculate_all_projections
# ----------------------------

# File paths (remain the same)
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
FALLBACK_FILE = Path("data/cleaned/batters_today.csv")
OUTPUT_FILE = Path("data/end_chain/complete/batter_props_projected.csv")

def project_batter_props(df: pd.DataFrame, pitchers: pd.DataFrame, context: str, fallback: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrates the batter projection process for a given DataFrame.
    """
    # Step 1: Preprocess data (merge with pitchers, apply fallback)
    df_prepared = merge_with_pitcher_data(df, pitchers, context)
    df_prepared = apply_batter_fallback_stats(df_prepared, fallback)

    # Step 2: Calculate all projections
    df_projected = calculate_all_projections(df_prepared)

    # Step 3: Add final metadata and select columns
    df_projected["prop_type"] = "total_bases"
    df_projected["context"] = context
    df_projected = df_projected.rename(columns={"batter_name": "name"})

    return df_projected[[
        "name", "team", "projected_total_bases", "projected_hits",
        "projected_singles", "projected_walks", "b_rbi", "prop_type", "context"
    ]]

def main():
    print("🔄 Loading input files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    fallback = load_csv(FALLBACK_FILE)

    print("📊 Projecting props for home batters...")
    home_proj = project_batter_props(bat_home, pitchers, "home", fallback)

    print("📊 Projecting props for away batters...")
    away_proj = project_batter_props(bat_away, pitchers, "away", fallback)

    full = pd.concat([home_proj, away_proj], ignore_index=True)
    full.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Projections saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

