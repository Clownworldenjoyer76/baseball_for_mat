
import pandas as pd
import os

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def main():
    games = pd.read_csv(GAMES_FILE)
    stadium = pd.read_csv(STADIUM_FILE)

    # Ensure clean strings for joining
    games['home_team'] = games['home_team'].str.strip()
    stadium['home_team'] = stadium['home_team'].str.strip()

    # Columns to keep from stadium file
    stadium = stadium[[
        "home_team", "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude"
    ]]

    # Merge game data with stadium metadata
    merged = games.merge(stadium, on="home_team", how="inner")

    # Select columns for output
    output = merged[[
        "home_team", "game_time", "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude"
    ]]

    os.makedirs("data", exist_ok=True)
    output.to_csv(OUTPUT_FILE, index=False)

    # Write summary
    with open(SUMMARY_FILE, "w") as f:
        f.write(f"✅ Created: {OUTPUT_FILE}\n")
        f.write(f"✅ Summary written to: {SUMMARY_FILE}\n")
        f.write(f"✅ Total records written: {len(output)}\n")

    print(f"✅ Created: {OUTPUT_FILE}")
    print(f"✅ Summary written to: {SUMMARY_FILE}")

if __name__ == "__main__":
    main()
