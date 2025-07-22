import pandas as pd

# File paths
WEATHER_FILE = "data/weather_adjustments.csv"
INPUT_FILE = "data/weather_input.csv"

def main():
    # Load both CSVs
    weather_df = pd.read_csv(WEATHER_FILE)
    input_df = pd.read_csv(INPUT_FILE)

    # Sanity check
    if 'stadium' not in weather_df.columns or 'away_team' not in input_df.columns or 'stadium' not in input_df.columns:
        raise ValueError("Missing required columns in one of the input files.")

    # Set home_team = stadium (copy column)
    weather_df['home_team'] = weather_df['stadium']

    # Map: stadium → away_team (from weather_input.csv)
    away_team_map = input_df.set_index('stadium')['away_team'].to_dict()
    weather_df['away_team'] = weather_df['stadium'].map(away_team_map)

    # Save output (overwrite)
    weather_df.to_csv(WEATHER_FILE, index=False)

    # Log
    print(f"✅ Updated {WEATHER_FILE} with 'home_team' and 'away_team' columns.")
    print(weather_df[['stadium', 'home_team', 'away_team']].head())

if __name__ == "__main__":
    main()
