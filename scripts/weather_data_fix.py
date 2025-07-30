# scripts/weather_data_fix.py

import pandas as pd
from pathlib import Path

# File path
WEATHER_FILE = Path("data/weather_adjustments.csv")

def calculate_weather_factor(df: pd.DataFrame) -> pd.Series:
    factor = pd.Series(1.0, index=df.index)

    # Lower factor inside dome (neutral environment)
    factor[df["notes"] == "Roof closed"] = 1.00

    # Adjust for temperature
    factor += (df["temperature"] - 70) * 0.005
    factor = factor.clip(lower=0.85, upper=1.15)

    # Adjust for wind
    out_wind = (df["wind_direction"] == "out")
    in_wind = (df["wind_direction"] == "in")

    factor[out_wind] += df["wind_speed"] * 0.01
    factor[in_wind] -= df["wind_speed"] * 0.01

    return factor.clip(lower=0.80, upper=1.20).round(3)

def main():
    print("🔄 Loading weather adjustments...")
    df = pd.read_csv(WEATHER_FILE)

    if "weather_factor" not in df.columns:
        print("🧮 Calculating weather_factor...")
        df["weather_factor"] = calculate_weather_factor(df)
    else:
        print("✅ weather_factor already exists. Skipping calculation.")

    print("💾 Saving updated file...")
    df.to_csv(WEATHER_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()
