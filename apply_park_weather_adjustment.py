import pandas as pd

def apply_adjustments(batters, weather, park_day, park_night):
    # Merge weather data
    batters = pd.merge(batters, weather, on="venue", how="left")

    # Merge park factor data
    day_merge = pd.merge(batters, park_day, on="venue", how="left", suffixes=('', '_day'))
    final = pd.merge(day_merge, park_night, on="venue", how="left", suffixes=('', '_night'))

    # Fill NAs with 1 for multiplicative adjustments
    final.fillna(1, inplace=True)

    # Apply basic adjustment logic (example using wOBA)
    final["woba_adjusted"] = (
        final["woba"] *
        final["temp_adjustment"] *
        final["wind_adjustment"] *
        final["park_factor_day"] *
        final["park_factor_night"]
    )
    return final

def main():
    print("Loading data...")
    batters = pd.read_csv("data/cleaned/batters_normalized_cleaned.csv")
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"Batters: {len(batters)}, Weather: {len(weather)}, Parks: {len(park_day)}")

    print("Applying adjustments to batters...")
    adjusted = apply_adjustments(batters, weather, park_day, park_night)

    output_path = "data/adjusted/batters_weather_park_adjusted.csv"
    Path("data/adjusted").mkdir(parents=True, exist_ok=True)
    adjusted.to_csv(output_path, index=False)
    print(f"✅ Done. Adjusted batter file written to {output_path}")

if __name__ == "__main__":
    main()
