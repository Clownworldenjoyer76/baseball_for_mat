
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

    # Join each batter row to opposing pitcher (batters['team'] vs pitchers['team_x'])
    merged = batters.merge(
        pitchers,
        left_on="team",
        right_on="team_x",
        suffixes=("", "_pitch"),
        how="left"
    )

    print("🧮 Computing final team-level score projection...")
    merged["adjusted_score"] = (
        merged["total_hits_projection"].fillna(0)
        + merged["avg_hr"].fillna(0) * 1.5
        + merged["total_bases_projection"].fillna(0) * 0.25
    )

    team_scores = (
        merged.groupby("team")["adjusted_score"]
        .sum()
        .reset_index()
        .rename(columns={"adjusted_score": "projected_team_score"})
    )

    print("💾 Saving final score output to:", OUTPUT_FILE)
    team_scores.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
