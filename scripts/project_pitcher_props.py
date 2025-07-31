
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

    print("🔍 Filtering xtra columns needed for projection...")
    required_cols = [
        "last_name, first_name", "hit", "hr", "slg", "woba", 
        "era", "xfip", "whip", "k_percent", "bb_percent"
    ]
    df_xtra = df_xtra[[col for col in required_cols if col in df_xtra.columns]]

    print("🔗 Merging xtra stats into final (many-to-one)...")
    df = df_final.merge(df_xtra, on="last_name, first_name", how="left", validate="many_to_one")

    print("✅ Running projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving output to:", OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
