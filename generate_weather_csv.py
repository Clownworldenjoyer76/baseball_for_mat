
import pandas as pd
import os

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_CSV = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def write_summary(message):
    os.makedirs("data", exist_ok=True)
    with open(SUMMARY_FILE, "w") as f:
        f.write(message)

def main():
    try:
        tp = pd.read_csv(PITCHERS_FILE)
        sm = pd.read_csv(STADIUM_FILE)

        # Normalize columns
        tp['home_team'] = tp['home_team'].str.strip().str.lower()
        sm['home_team'] = sm['home_team'].str.strip().str.lower()

        required_columns = [
            "home_team", "game_time", "venue", "city", "state",
            "timezone", "is_dome", "latitude", "longitude"
        ]

        if not all(col in tp.columns for col in ["home_team", "game_time"]):
            write_summary("Missing required columns in todays_pitchers.csv")
            return
        if not all(col in sm.columns for col in required_columns if col != "game_time"):
            write_summary("Missing required columns in stadium_metadata.csv")
            return

        # Merge on home_team (inner join)
        merged = pd.merge(tp, sm, on="home_team", how="inner")

        output = merged[required_columns]
        os.makedirs("data", exist_ok=True)
        output.to_csv(OUTPUT_CSV, index=False)

        write_summary(f"✅ Created: {OUTPUT_CSV}\nRows: {len(output)}")

    except Exception as e:
        write_summary(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
