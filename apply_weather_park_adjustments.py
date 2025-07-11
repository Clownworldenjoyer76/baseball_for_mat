import pandas as pd

def apply_adjustments(batters, weather, park_day, park_night):
    park_day.rename(columns={"venue": "stadium"}, inplace=True)
    park_night.rename(columns={"venue": "stadium"}, inplace=True)
    weather = weather.drop_duplicates(subset='stadium')

    df = pd.merge(weather, park_day, on="stadium", how="left", suffixes=("", "_day"))
    df = pd.merge(df, park_night, on="stadium", how="left", suffixes=("", "_night"))

    # Dummy adjustment logic for illustration
    batters['adjusted_hits'] = batters['hit'] * 1.1  # Placeholder
    return batters

def main():
    print("Loading data...")
    batters = pd.read_csv("data/cleaned/batters_normalized_cleaned.csv")
    pitchers = pd.read_csv("data/cleaned/pitchers_normalized_cleaned.csv")
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")
    print("Applying adjustments to batters...")
    adjusted = apply_adjustments(batters, weather, park_day, park_night)
    adjusted.to_csv("data/adjusted/batters_adjusted.csv", index=False)
    print("Adjustments complete.")

if __name__ == "__main__":
    main()
