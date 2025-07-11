
import pandas as pd
from pathlib import Path
from apply_adjustments import apply_adjustments

def main():
    print("Loading data...")
    batters = pd.read_csv("data/cleaned/batters_normalized_cleaned.csv")
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"Batters: {len(batters)}, ParkDay: {len(park_day)}, Weather: {len(weather)}")
    print("Applying adjustments to batters...")

    adjusted = apply_adjustments(batters, weather, park_day, park_night)

    output_path = Path("data/adjusted")
    output_path.mkdir(parents=True, exist_ok=True)
    adjusted.to_csv(output_path / "batters_adjusted_weather_park.csv", index=False)
    print("✅ Done. Adjusted batter file written to data/adjusted/")

if __name__ == "__main__":
    main()
