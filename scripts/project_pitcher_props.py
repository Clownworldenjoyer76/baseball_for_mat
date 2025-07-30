import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections
from utils import safe_col

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_props_projected.csv")

def main():
    print("🔄 Loading pitcher files...")
    df_final = pd.read_csv(FINAL_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    print("🧼 Cleaning & aligning columns...")
    df_final["last_name, first_name"] = df_final["last_name, first_name"].astype(str).str.strip().str.title()
    df_xtra["last_name, first_name"] = df_xtra["last_name, first_name"].astype(str).str.strip().str.title()

    print("🔗 Merging...")
    df = df_final.merge(df_xtra, on="last_name, first_name", how="left")

    print("📈 Applying projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving to:", OUTPUT_FILE)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()
