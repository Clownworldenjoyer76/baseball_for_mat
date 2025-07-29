# scripts/project_batter_props.py

import pandas as pd
from pathlib import Path
import os

# --- Config ---
BAT_HOME_PATH = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_PATH = Path("data/end_chain/final/updating/bat_away3.csv")
PITCHER_PATH = Path("data/end_chain/final/startingpitchers.csv")
OUTPUT_PATH = Path("data/end_chain/complete/projected_batter_props.csv")

# --- Loaders ---
def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

# --- Projection Logic ---
def project_batter_props(df, pitchers_df, team_type):
    df = df.copy()
    pitcher_key = "pitcher_home" if team_type == "away" else "pitcher_away"
    team_key = "away_team" if team_type == "away" else "home_team"

    df = df.merge(
        pitchers_df[["team", "throws"]],
        left_on=pitcher_key,
        right_on="team",
        how="left",
        suffixes=('', '_pitcher')
    )

    df["throws"] = df["throws"].fillna("R")
    df["bats"] = df["bats"].fillna("R")

    # Base prop using weather-adjusted wOBA
    df["base_woba"] = df["adj_woba_weather"].fillna(df["woba"])

    # Same-handed penalty
    df["woba_vs_pitcher"] = df["base_woba"]
    same_hand = df["bats"] == df["throws"]
    df.loc[same_hand, "woba_vs_pitcher"] *= 0.92  # modest penalty vs same-handed pitcher

    # Park factor
    df["park_multiplier"] = df["Park Factor"].fillna(100) / 100
    df["final_woba_adj"] = df["woba_vs_pitcher"] * df["park_multiplier"]

    # Project props (example: total bases proxy, 1.8x wOBA)
    df["proj_total_bases"] = (df["final_woba_adj"] * 1.8).round(2)
    df["proj_hits"] = (df["final_woba_adj"] * 1.1).round(2)
    df["proj_runs+rbi"] = (df["final_woba_adj"] * 2.2).round(2)

    # Clean up
    df["team_type"] = team_type
    return df[[
        "batter", "team_type", team_key, pitcher_key,
        "bats", "throws", "final_woba_adj",
        "proj_total_bases", "proj_hits", "proj_runs+rbi"
    ]].rename(columns={team_key: "team", pitcher_key: "pitcher"})

# --- Main ---
def main():
    os.makedirs(OUTPUT_PATH.parent, exist_ok=True)

    bat_home = load_csv(BAT_HOME_PATH)
    bat_away = load_csv(BAT_AWAY_PATH)
    pitchers = load_csv(PITCHER_PATH)

    home_proj = project_batter_props(bat_home, pitchers, "home")
    away_proj = project_batter_props(bat_away, pitchers, "away")

    full = pd.concat([home_proj, away_proj], ignore_index=True)
    full.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Projected batter props saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
