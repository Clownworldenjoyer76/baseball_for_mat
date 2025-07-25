import pandas as pd
from pathlib import Path

# File paths
BATT_FILE = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
GAMES_FILE = Path("data/end_chain/todaysgames_normalized.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    # Load files
    batters = load_csv(BATT_FILE)
    games = load_csv(GAMES_FILE)
    weather = load_csv(WEATHER_FILE)

    # Merge home_team from games
    games_subset = games[["away_team", "home_team"]].drop_duplicates()
    batters = pd.merge(batters, games_subset, left_on="team", right_on="away_team", how="left")
    batters.drop(columns=["away_team"], inplace=True)

    # Merge weather columns
    weather_subset = weather[["away_team", "home_team", "temp", "humidity", "wind_speed", "wind_direction", "Park Factor", "time_of_day"]]
    batters = pd.merge(batters, weather_subset, on=["team", "home_team"], how="left")

    # Save updated
    batters.to_csv(BATT_FILE, index=False)
    print(f"✅ Updated {BATT_FILE.name}")

if __name__ == "__main__":
    main()
