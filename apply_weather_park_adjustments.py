import pandas as pd
import os

# File paths
BATTER_FILE = "data/cleaned/batters_normalized_cleaned.csv"
PITCHER_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
WEATHER_FILE = "data/weather_adjustments.csv"
DAY_PARK_FILE = "data/Data/park_factors_day.csv"
NIGHT_PARK_FILE = "data/Data/park_factors_night.csv"
OUTPUT_FILE = "data/adjusted_projections.csv"

def load_data():
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    day_park = pd.read_csv(DAY_PARK_FILE)
    night_park = pd.read_csv(NIGHT_PARK_FILE)
    return batters, pitchers, weather, day_park, night_park

def apply_adjustments(df, weather, park_factors):
    df = df.merge(weather, on="stadium", how="left")
    df = df.merge(park_factors, on="stadium", how="left")

    for stat in ["HR", "H", "RBI"]:
        weather_col = f"{stat}_weather_modifier"
        park_col = f"{stat}_park_modifier"

        if weather_col in df.columns and park_col in df.columns:
            df[f"{stat}_adjusted"] = df[stat] * (1 + df[weather_col]/100) * (1 + df[park_col]/100)

    return df

def main():
    print("Loading data...")
    batters, pitchers, weather, day_park, night_park = load_data()
    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")

    # Choose park factors (for now, default to day)
    park_factors = day_park

    print("Applying adjustments to batters...")
    batters_adj = apply_adjustments(batters, weather, park_factors)

    print("Applying adjustments to pitchers...")
    pitchers_adj = apply_adjustments(pitchers, weather, park_factors)

    print("Combining and saving...")
    combined = pd.concat([batters_adj, pitchers_adj], ignore_index=True)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved adjusted projections to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
