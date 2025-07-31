
import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
CLEANED_FILE = Path("data/cleaned/pitchers_normalized_cleaned.csv")
XTRA_FILE = Path("data/end_chain/pitchers_xtra.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_props_projected.csv")

def main():
    print("🔄 Loading pitcher base + enriched files...")
    df_final = pd.read_csv(FINAL_FILE)
    df_cleaned = pd.read_csv(CLEANED_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    # Force player_id to string for consistency
    for df in [df_final, df_cleaned, df_xtra]:
        df["player_id"] = df["player_id"].astype(str)

    # Rename mapped fields
    df_cleaned.rename(columns={
        "home_run": "hr",
        "slg_percent": "slg"
    }, inplace=True)

    print("🔗 Merging cleaned stats on player_id...")
    df = df_final.merge(df_cleaned, on="player_id", how="left")

    print("🔗 Merging earned runs on player_id...")
    df = df.merge(df_xtra[["player_id", "p_earned_run"]], on="player_id", how="left")

    print("🧮 Calculating ERA using innings_pitched...")
    df["era"] = (df["p_earned_run"] / df["innings_pitched"]) * 9
    df["era"] = df["era"].fillna(0).round(2)

    print("✅ Running projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving output to:", OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
