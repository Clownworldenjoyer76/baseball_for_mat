import pandas as pd
from pathlib import Path
from projection_formulas import project_final_score

# File paths
BATTER_PROPS_FILE = Path("data/_projections/batter_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    print("🔄 Loading projected batter data...")
    df = pd.read_csv(BATTER_PROPS_FILE)

    print("🧠 Projecting final scores...")
    df["team"] = df.apply(lambda row: row["home_team"] if row["team_type"] == "home" else row["away_team"], axis=1)
    final_df = project_final_score(df)

    print("💾 Saving to:", OUTPUT_FILE)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUTPUT_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()