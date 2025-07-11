
import pandas as pd

# Input files
PARK_DAY = "data/Data/park_factors_day.csv"
PARK_NIGHT = "data/Data/park_factors_night.csv"
WEATHER = "data/weather_adjustments.csv"
BATTERS = "data/cleaned/batters_normalized_cleaned.csv"
PITCHERS = "data/cleaned/pitchers_normalized_cleaned.csv"

# Output file
OUTPUT = "data/adjusted_projections.csv"

def main():
    # Load data
    park_day = pd.read_csv(PARK_DAY)
    park_night = pd.read_csv(PARK_NIGHT)
    weather = pd.read_csv(WEATHER)
    batters = pd.read_csv(BATTERS)
    pitchers = pd.read_csv(PITCHERS)

    # Drop duplicate weather entries per stadium
    weather = weather.drop_duplicates(subset=["stadium"], keep="first")

    # Placeholder weights
    WEATHER_WEIGHT = 0.3
    PARK_WEIGHT = 0.7

    # Merge batter and pitcher data
    merged = pd.merge(batters, pitchers, on=["game_id", "stadium"], how="inner")

    # Merge with weather and park data (example: weather only)
    merged = pd.merge(merged, weather, on="stadium", how="left")

    # Placeholder for adjusted HR (example logic)
    if "HR" in merged.columns:
        merged["HR_adjusted"] = merged["HR"] * (1 + WEATHER_WEIGHT)

    # Output
    merged.to_csv(OUTPUT, index=False)
    print(f"✅ Adjusted projections saved to {OUTPUT}")

if __name__ == "__main__":
    main()
