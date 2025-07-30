import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections
from utils import safe_col

# File paths
HOME_FILE = Path("data/end_chain/final/batter_home_final.csv")
AWAY_FILE = Path("data/end_chain/final/batter_away_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/batters_xtra_normalized.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_projected.csv")

def main():
    print("🔄 Loading batter files...")
    df_home = pd.read_csv(HOME_FILE)
    df_away = pd.read_csv(AWAY_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    print("➕ Combining home + away batters...")
    df_final = pd.concat([df_home, df_away], ignore_index=True)

    print("🧼 Cleaning & aligning columns...")
    df_final["last_name, first_name"] = df_final["last_name, first_name"].astype(str).str.strip().str.title()
    df_xtra["last_name, first_name"] = df_xtra["last_name, first_name"].astype(str).str.strip().str.title()

    print("🔗 Merging with extra columns...")
    df = df_final.merge(df_xtra, on="last_name, first_name", how="left")

    print("📈 Applying projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving to:", OUTPUT_FILE)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()
