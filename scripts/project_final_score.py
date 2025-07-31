
import pandas as pd
from pathlib import Path

# File paths
BATTER_FILE = Path("data/_projections/batter_props_projected.csv")
PITCHER_FILE = Path("data/_projections/pitcher_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    print("🔄 Loading projected batter and pitcher data...")
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)

    # Normalize join keys
    batters["team"] = batters["team"].str.strip().str.upper()
    pitchers["team"] = pitchers["team"].str.strip().str.upper()

    # Merge on team
    merged = batters.merge(pitchers, on="team", suffixes=("", "_pitch"), how="left")

    # Deduplicate on name + team to prevent inflated scoring
    merged = merged.drop_duplicates(subset=["name", "team"])

    # Compute rebalanced adjusted score
    merged["adjusted_score"] = (
        merged["total_hits_projection"].fillna(0)
        + merged["avg_hr"].fillna(0) * 1.1
        + merged["total_bases_projection"].fillna(0) * 0.1
    )

    # Aggregate and normalize to 137.5 total runs
    team_scores = (
        merged.groupby("team")["adjusted_score"]
        .sum()
        .reset_index()
    )
    scale = 137.5 / team_scores["adjusted_score"].sum()
    team_scores["projected_team_score"] = (team_scores["adjusted_score"] * scale).round(2)
    team_scores.drop(columns="adjusted_score", inplace=True)

    print("💾 Saving final score output to:", OUTPUT_FILE)
    team_scores.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
