import pandas as pd

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_teams.csv"
WEATHER_ADJUSTMENTS_FILE = "data/weather_adjustments.csv"

def create_weather_teams_file():
    # Load input
    df = pd.read_csv(INPUT_FILE)

    # Rename team_name_x → home_team
    df.rename(columns={"team_name_x": "home_team"}, inplace=True)

    # Select only required columns
    output_df = df[["home_team", "away_team"]]

    # Save output
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote home_team and away_team to {OUTPUT_FILE}")

def rename_stadium_to_venue():
    try:
        df = pd.read_csv(WEATHER_ADJUSTMENTS_FILE)

        if "stadium" in df.columns:
            df.rename(columns={"stadium": "venue"}, inplace=True)
            df.to_csv(WEATHER_ADJUSTMENTS_FILE, index=False)
            print(f"✅ Renamed 'stadium' to 'venue' in {WEATHER_ADJUSTMENTS_FILE}")
        else:
            print(f"⚠️ Column 'stadium' not found in {WEATHER_ADJUSTMENTS_FILE}")
    except Exception as e:
        print(f"❌ Failed to update {WEATHER_ADJUSTMENTS_FILE}: {e}")

def main():
    create_weather_teams_file()
    rename_stadium_to_venue() # Changed function call here

if __name__ == "__main__":
    main()
