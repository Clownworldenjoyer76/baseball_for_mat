# scripts/project_batter_props.py

import pandas as pd
from pathlib import Path
import os

# --- File Paths ---
HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
AWAY_FILE = Path("data/end_chain/final/updating/bat_away3.csv")
PITCHERS_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_DIR = Path("data/end_chain/complete")
OUTPUT_FILE = OUTPUT_DIR / "batter_prop_projections.csv"

# --- Utility ---
def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip().rstrip(',')
    return name.lower()

# --- Prop Projection Logic ---
def project_batter_props(df: pd.DataFrame, pitchers_df: pd.DataFrame, side: str) -> pd.DataFrame:
    pitcher_col = f"pitcher_{side}"

    # Normalize for join
    df["pitcher_normalized"] = df[pitcher_col].astype(str).apply(normalize_name)
    pitchers_df["name_normalized"] = pitchers_df["name"].astype(str).apply(normalize_name)

    # Merge with pitcher data
    df = df.merge(pitchers_df, how="left", left_on="pitcher_normalized", right_on="name_normalized", suffixes=("", "_pitcher"))

    # Basic projection logic
    df["projected_total_bases"] = (
        df.get("adj_woba_combined", 0) * 4 +
        df.get("hit_weather", 0) * 1.5 +
        df.get("innings_pitched", 0).fillna(0) * 0.1 -
        df.get("walks", 0).fillna(0) * 0.05
    ).round(2)

    df["projected_hits"] = (
        df.get("adj_woba_weather", 0) * 3 +
        df.get("whiff%", 0).fillna(0) * -0.1 +
        df.get("strikeouts", 0).fillna(0) * -0.05
    ).round(2)

    df["projected_rbis"] = (
        df.get("adj_woba_combined", 0) * 2 +
        df.get("earned_runs", 0).fillna(0) * 0.25
    ).round(2)

    df["side"] = side
    return df

# --- Main Execution ---
def main():
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True)

    if not (HOME_FILE.exists() and AWAY_FILE.exists() and PITCHERS_FILE.exists()):
        print("❌ Missing input files.")
        return

    bat_home = pd.read_csv(HOME_FILE)
    bat_away = pd.read_csv(AWAY_FILE)
    pitchers = pd.read_csv(PITCHERS_FILE)

    home_proj = project_batter_props(bat_home, pitchers, "home")
    away_proj = project_batter_props(bat_away, pitchers, "away")

    combined = pd.concat([home_proj, away_proj], ignore_index=True)
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Prop projections saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
