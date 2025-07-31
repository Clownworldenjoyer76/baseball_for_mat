
import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_props_projected.csv")

def normalize(df):
    df["last_name, first_name"] = df["last_name, first_name"].astype(str).str.strip().str.title()
    return df

def main():
    print("🔄 Loading pitcher file...")
    df = normalize(pd.read_csv(FINAL_FILE))

    print("✅ Running projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving output to:", OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
