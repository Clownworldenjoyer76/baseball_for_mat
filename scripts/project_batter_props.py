
import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections

# File paths
AWAY_FILE = Path("data/end_chain/final/batter_away_final.csv")
HOME_FILE = Path("data/end_chain/final/batter_home_final.csv")
TODAY_FILE = Path("data/end_chain/final/bat_today_final.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_projected.csv")

def normalize(df):
    df["last_name, first_name"] = df["last_name, first_name"].astype(str).str.strip().str.title()
    return df

def main():
    print("🔄 Loading batter files...")
    df_away = normalize(pd.read_csv(AWAY_FILE))
    df_home = normalize(pd.read_csv(HOME_FILE))
    df_today = normalize(pd.read_csv(TODAY_FILE))

    print("🧬 Merging data sources...")
    df = pd.concat([df_home, df_away], ignore_index=True)
    df = df.merge(df_today, on="last_name, first_name", how="inner")

    print("✅ Running projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving output to:", OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
