import pandas as pd

WEATHER_FILE = "data/weather_adjustments.csv"
WEATHER_INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"

def main():
    weather_df = pd.read_csv(WEATHER_FILE)
    input_df = pd.read_csv(WEATHER_INPUT_FILE)

    # Confirm required columns exist
    if "stadium" not in weather_df.columns or "away_team" not in input_df.columns:
        raise ValueError("Missing required columns in one of the input files.")

    # Copy stadium as home_team
    weather_df["home_team"] = weather_df["stadium"]

    # Map away_team using the stadium (which is also home_team)
    away_team_map = dict(zip(input_df["home_team"], input_df["away_team"]))
    weather_df["away_team"] = weather_df["stadium"].map(away_team_map)

    weather_df.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Added home_team and away_team columns to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
