import pandas as pd
from pathlib import Path

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/prop_candidates.csv"

def main():
    df = pd.read_csv(INPUT_FILE)

    # Keep only rows where any version of player name is populated
    name_cols = [col for col in df.columns if "name" in col.lower()]
    df["has_name"] = df[name_cols].notna().any(axis=1)

    # Only include rows that have a name and a valid adj_woba_combined
    props = df[df["has_name"] & df["adj_woba_combined"].notna()].copy()

    # Add prop-specific tag
    props["type"] = "prop"

    # Normalize one name column for consistency
    if "last_name, first_name_weather" in props.columns:
        props.rename(columns={"last_name, first_name_weather": "name"}, inplace=True)
    elif "name" not in props.columns:
        props["name"] = "unknown"

    # Normalize team casing
    if "team" in props.columns:
        props["team"] = props["team"].str.title()

    # Output
    props.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved prop candidates to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
