
import pandas as pd

# File paths
BATTER_FILE = 'data/cleaned/batters_normalized_cleaned.csv'
PITCHER_FILE = 'data/cleaned/pitchers_normalized_cleaned.csv'
WEATHER_FILE = 'data/weather_adjustments.csv'
PARK_DAY_FILE = 'data/Data/park_factors_day.csv'
PARK_NIGHT_FILE = 'data/Data/park_factors_night.csv'
OUTPUT_FILE = 'data/adjusted_projections.csv'

def apply_adjustments(df, weather, park_factors):
    print("Weather columns:", weather.columns.tolist())  # Debug output
    df = df.merge(weather, on="stadium", how="left")
    df = df.merge(park_factors, on="stadium", how="left")
    return df

def main():
    print("Loading data...")
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    park_day = pd.read_csv(PARK_DAY_FILE)
    park_night = pd.read_csv(PARK_NIGHT_FILE)

    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")

    print("Applying adjustments to batters...")
    batters_adj = apply_adjustments(batters, weather, park_day)

    print("Applying adjustments to pitchers...")
    pitchers_adj = apply_adjustments(pitchers, weather, park_night)

    adjusted = pd.concat([batters_adj, pitchers_adj], ignore_index=True)
    adjusted.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved adjusted projections to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
