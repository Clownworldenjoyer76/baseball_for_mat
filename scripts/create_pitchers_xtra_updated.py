# scripts/create_pitchers_xtra_updated.py

import pandas as pd
from pathlib import Path

# Define paths
INPUT_FILE = Path("data/end_chain/cleaned/games_cleaned.csv")
OUTPUT_FILE = Path("data/end_chain/cleaned/pitchers_xtra_updated.csv")

# Columns for output file
OUTPUT_COLUMNS = [
    "pitcher_home", "pitcher_away", "team", "name",
    "innings_pitched", "strikeouts", "walks", "earned_runs"
]

def main():
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)

    required_cols = {"pitcher_home", "pitcher_away", "team"}
    if not required_cols.issubset(df.columns):
        print(f"❌ Input file missing required columns: {required_cols - set(df.columns)}")
        return

    # Initialize new DataFrame
    output_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    output_df["pitcher_home"] = df["pitcher_home"]
    output_df["pitcher_away"] = df["pitcher_away"]
    output_df["team"] = df["team"]

    # Leave other fields blank
    for col in ["name", "innings_pitched", "strikeouts", "walks", "earned_runs"]:
        output_df[col] = ""

    # Save the file
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Created {OUTPUT_FILE} with shape {output_df.shape}")

if __name__ == "__main__":
    main()
