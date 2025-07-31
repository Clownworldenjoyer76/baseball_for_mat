
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

    for col in ["game_id", "team", "opponent", "projected_final_score"]:
        if col in bat.columns:
            bat[col] = bat[col].astype(str).str.strip()
        if col in pit.columns:
            pit[col] = pit[col].astype(str).str.strip()

    print("🔁 Merging each batter with their opposing pitcher by game_id + opponent...")
    merged = bat.merge(
        pit,
        left_on=["game_id", "opponent"],
        right_on=["game_id", "team"],
        suffixes=("_batter", "_pitcher"),
        how="left"
    )

    print("✅ Applying final score projection formula...")
    merged = project_final_score(merged)

    print("📊 Aggregating projected scores by game_id + batter's team...")
    output = (
        merged.groupby(["game_id", "team_batter"])["projected_final_score"]
        .mean()
        .reset_index()
        .rename(columns={"team_batter": "team"})
    )

    print("💾 Saving to:", OUTPUT_FILE)
    output.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
