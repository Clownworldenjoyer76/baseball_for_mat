import pandas as pd
from pathlib import Path

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/prop_candidates.csv"

def main():
    df = pd.read_csv(INPUT_FILE)

    # Create 'name' column from 'last_name, first_name_weather'
    if "last_name, first_name_weather" in df.columns:
        df["name"] = df["last_name, first_name_weather"]
    else:
        print("❌ Required column 'last_name, first_name_weather' not found.")
        return

    # Filter where 'name' is not null and 'adj_woba_combined' is a valid number
    df = df[df["name"].notna() & df["adj_woba_combined"].notna()]

    # Add 'type' column
    df["type"] = "prop"

    # Capitalize 'team' if it's lowercase
    if "team" in df.columns:
        df["team"] = df["team"].str.title()

    # Save output
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Prop candidates written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
