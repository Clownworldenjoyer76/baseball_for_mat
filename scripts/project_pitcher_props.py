
import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_props_projected.csv")

def normalize(df):
    df["last_name, first_name"] = df["last_name, first_name"].astype(str).str.strip().str.title()
    return df

def main():
    print("🔄 Loading pitcher files...")
    df_final = normalize(pd.read_csv(FINAL_FILE))
    df_xtra = normalize(pd.read_csv(XTRA_FILE))

    print("🧬 Merging pitcher data...")
    df = df_final.merge(df_xtra, on="last_name, first_name", how="left")

    print("✅ Running projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving output to:", OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
