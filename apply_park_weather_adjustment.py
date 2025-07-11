
import pandas as pd
import os

def apply_adjustments(batters, weather, park_day, park_night):
    df = batters.copy()

    # Merge park factors by venue
    df = pd.merge(df, park_day, left_on='team', right_on='home_team', how='left', suffixes=('', '_day'))
    df = pd.merge(df, park_night, left_on='team', right_on='home_team', how='left', suffixes=('', '_night'))

    # Merge weather data by team/home_team
    df = pd.merge(df, weather, left_on='team', right_on='home_team', how='left')

    return df

def main():
    print("Loading data...")
    batters = pd.read_csv("data/cleaned/batters_normalized_cleaned.csv")
    pitchers = pd.read_csv("data/cleaned/pitchers_normalized_cleaned.csv")
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")
    print("Applying adjustments to batters...")

    batters_adj = apply_adjustments(batters, weather, park_day, park_night)

    os.makedirs("data/adjusted", exist_ok=True)
    batters_adj.to_csv("data/adjusted/batters_adjusted.csv", index=False)
    print("Done. Adjusted batter file written.")

if __name__ == "__main__":
    main()
