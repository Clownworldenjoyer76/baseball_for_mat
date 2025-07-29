# woba_calc.py

import pandas as pd
from pathlib import Path

def apply_woba_adjustments(file_path):
    df = pd.read_csv(file_path)

    # Calculate adj_woba_weather
    if "woba" in df.columns:
        df["adj_woba_weather"] = df["woba"]
        df.loc[df["temperature"].notna() & (df["temperature"] >= 85), "adj_woba_weather"] *= 1.03
        df.loc[df["temperature"].notna() & (df["temperature"] <= 50), "adj_woba_weather"] *= 0.97
    else:
        print(f"⚠️ 'woba' column missing in {file_path.name}, skipping adj_woba_weather")

    # Calculate adj_woba_combined
    if "adj_woba_weather" in df.columns and "adj_woba_park" in df.columns:
        df["adj_woba_combined"] = (df["adj_woba_weather"] + df["adj_woba_park"]) / 2
    else:
        print(f"⚠️ Missing columns for adj_woba_combined in {file_path.name}")

    # Overwrite the original file
    df.to_csv(file_path, index=False)
    print(f"✅ Updated: {file_path}")

def main():
    files = [
        Path("data/end_chain/final/updating/bat_home3.csv"),
        Path("data/end_chain/final/updating/bat_away3.csv")
    ]

    for file in files:
        if file.exists():
            apply_woba_adjustments(file)
        else:
            print(f"❌ File not found: {file}")

if __name__ == "__main__":
    main()
