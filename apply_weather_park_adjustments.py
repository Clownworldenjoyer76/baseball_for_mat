import pandas as pd

# Constants
BATTERS_FILE = 'data/cleaned/batters_normalized_cleaned.csv'
PITCHERS_FILE = 'data/cleaned/pitchers_normalized_cleaned.csv'
WEATHER_FILE = 'data/weather_adjustments.csv'
PARK_DAY_FILE = 'data/Data/park_factors_day.csv'
PARK_NIGHT_FILE = 'data/Data/park_factors_night.csv'
OUTPUT_FILE = 'data/adjusted_projections.csv'

def load_data():
    batters = pd.read_csv(BATTERS_FILE)
    pitchers = pd.read_csv(PITCHERS_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    park_day = pd.read_csv(PARK_DAY_FILE)
    park_night = pd.read_csv(PARK_NIGHT_FILE)
    return batters, pitchers, weather, park_day, park_night

def apply_adjustments(batters, weather, park_factors):
    print("🔍 Batters columns:", list(batters.columns))
    print("🔍 Weather columns:", list(weather.columns))
    df = pd.merge(weather, batters, on="stadium", how="left")
    df['adjusted_value'] = 100  # Placeholder for real logic
    return df

def main():
    print("Loading data...")
    batters, pitchers, weather, park_day, park_night = load_data()
    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")
    print("Applying adjustments to batters...")
    adjusted = apply_adjustments(batters, weather, park_day)
    adjusted.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Adjusted data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()