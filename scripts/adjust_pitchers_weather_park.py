
import pandas as pd
from pathlib import Path

def load_and_merge(pitchers_file, weather_file, park_day_file, park_night_file):
    df = pd.read_csv(pitchers_file)
    weather = pd.read_csv(weather_file).drop_duplicates(subset='home_team')
    park_day = pd.read_csv(park_day_file).drop_duplicates(subset='home_team')
    park_night = pd.read_csv(park_night_file).drop_duplicates(subset='home_team')

    df['home_team'] = df['team']

    df = pd.merge(df, weather, on="home_team", how="left")
    df = pd.merge(df, park_day, on="home_team", how="left", suffixes=('', '_day'))
    df = pd.merge(df, park_night, on="home_team", how="left", suffixes=('', '_night'))

    return df

def main():
    output_dir = Path("data/adjusted")
    output_dir.mkdir(parents=True, exist_ok=True)

    home_pitchers = load_and_merge(
        "data/adjusted/pitchers_home.csv",
        "data/weather_adjustments.csv",
        "data/Data/park_factors_day.csv",
        "data/Data/park_factors_night.csv"
    )
    home_pitchers.to_csv(output_dir / "pitchers_adjusted_home.csv", index=False)

    away_pitchers = load_and_merge(
        "data/adjusted/pitchers_away.csv",
        "data/weather_adjustments.csv",
        "data/Data/park_factors_day.csv",
        "data/Data/park_factors_night.csv"
    )
    away_pitchers.to_csv(output_dir / "pitchers_adjusted_away.csv", index=False)

    print("✅ Pitchers adjustment complete and saved.")

if __name__ == "__main__":
    main()
