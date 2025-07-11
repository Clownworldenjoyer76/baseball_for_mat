
import pandas as pd
import os

def apply_adjustments(batters, weather, park_day, park_night):
    print("Applying adjustments to batters...")
    df = pd.merge(batters, weather, on="stadium", how="left")
    df = pd.merge(df, park_day, on="stadium", how="left", suffixes=("", "_day"))
    df = pd.merge(df, park_night, on="stadium", how="left", suffixes=("", "_night"))
    # Dummy adjustment for demonstration
    df['adjusted'] = df['avg_hit_speed'] * 1.02
    return df

def main():
    print("Loading data...")
    batters = pd.read_csv("data/cleaned/batters_normalized_cleaned.csv")
    pitchers = pd.read_csv("data/cleaned/pitchers_normalized_cleaned.csv")
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")

    adjusted = apply_adjustments(batters, weather, park_day, park_night)

    # Ensure output directory exists
    os.makedirs("data/adjusted", exist_ok=True)
    adjusted.to_csv("data/adjusted/batters_adjusted.csv", index=False)
    print("Saved adjusted batter projections.")

if __name__ == "__main__":
    main()
