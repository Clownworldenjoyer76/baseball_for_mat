import pandas as pd
import os

PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_CSV = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_input_summary.txt"

def main():
    try:
        tp = pd.read_csv(PITCHERS_FILE)
        sm = pd.read_csv(STADIUM_FILE)

        if "home_team" not in tp.columns or "game_time" not in tp.columns:
            raise ValueError("Missing required columns in todays_pitchers.csv")

        required_stadium_cols = ["home_team", "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude"]
        for col in required_stadium_cols:
            if col not in sm.columns:
                raise ValueError(f"Missing required column in stadium_metadata.csv: {col}")

        tp["home_team"] = tp["home_team"].str.strip().str.lower()
        sm["home_team"] = sm["home_team"].str.strip().str.lower()

        sm_filtered = sm[sm["home_team"].isin(tp["home_team"])]

        output_df = pd.merge(
            tp[["home_team", "game_time"]],
            sm_filtered[required_stadium_cols],
            on="home_team",
            how="inner"
        )

        output_df.to_csv(OUTPUT_CSV, index=False)

        with open(SUMMARY_FILE, "w") as f:
            f.write(f"✅ weather_input.csv created with {len(output_df)} rows\n")
            f.write("Included columns: home_team, game_time, venue, city, state, timezone, is_dome, latitude, longitude\n")
            f.write(f"Teams included: {', '.join(output_df['home_team'].unique())}\n")

        print("✅ CSV and summary file generated successfully.")

    except Exception as e:
        with open(SUMMARY_FILE, "w") as f:
            f.write(f"❌ Error: {str(e)}\n")
        print(f"❌ Failed: {str(e)}")

if __name__ == "__main__":
    main()