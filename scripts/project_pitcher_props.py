
import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
CLEANED_FILE = Path("data/cleaned/pitchers_normalized_cleaned.csv")
XTRA_FILE = Path("data/end_chain/pitchers_xtra.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_props_projected.csv")

def normalize(df):
    df["last_name, first_name"] = df["last_name, first_name"].astype(str).str.strip().str.title()
    return df

def main():
    print("🔄 Loading pitcher base + enriched files...")
    df_final = normalize(pd.read_csv(FINAL_FILE))
    df_cleaned = normalize(pd.read_csv(CLEANED_FILE))
    df_xtra = normalize(pd.read_csv(XTRA_FILE))

    # Rename mapped fields
    df_cleaned.rename(columns={
        "home_run": "hr",
        "slg_percent": "slg"
    }, inplace=True)

    print("🔗 Merging normalized stats...")
    df = df_final.merge(df_cleaned, on="last_name, first_name", how="left")
    df = df.merge(df_xtra[["last_name, first_name", "p_earned_run", "p_formatted_ip"]], on="last_name, first_name", how="left")

    print("🧮 Calculating ERA from earned runs and IP...")
    df["era"] = (df["p_earned_run"] / df["p_formatted_ip"]) * 9
    df["era"] = df["era"].fillna(0).round(2)

    print("✅ Running projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving output to:", OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
