
import pandas as pd
from pathlib import Path
from projection_formulas import project_final_score

# File paths
BATTER_FILE = Path("data/_projections/batter_props_projected.csv")
PITCHER_FILE = Path("data/_projections/pitcher_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    print("🔄 Loading batter and pitcher projections...")
    bat = pd.read_csv(BATTER_FILE)
    pit = pd.read_csv(PITCHER_FILE)

    print("✅ Applying final score projection formula...")
    bat = project_final_score(bat)

    print("📊 Aggregating by team and game_id...")
    required_cols = ["game_id", "team", "projected_final_score"]
    if not all(col in bat.columns for col in required_cols):
        missing = [col for col in required_cols if col not in bat.columns]
        raise ValueError(f"Missing required columns in batter file: {missing}")

    team_scores = (
        bat.groupby(["game_id", "team"])["projected_final_score"]
        .mean()
        .reset_index()
        .rename(columns={"projected_final_score": "team_avg_score"})
    )

    print("⚾ Merging with pitcher props for reference...")
    if "game_id" in pit.columns and "team" in pit.columns:
        team_scores = team_scores.merge(
            pit[["game_id", "team"]].drop_duplicates(),
            on=["game_id", "team"],
            how="left"
        )

    print("💾 Saving final score projections to:", OUTPUT_FILE)
    team_scores.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
