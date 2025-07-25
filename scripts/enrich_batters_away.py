import pandas as pd
from pathlib import Path

# Input paths
BATTERS_FILE = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")

# Load input files
batters = pd.read_csv(BATTERS_FILE)
weather = pd.read_csv(WEATHER_FILE)

# Required columns from weather
weather_cols = [
    "home_team",
    "away_team",
    "temperature",
    "wind_speed",
    "wind_direction",
    "humidity",
    "condition",
    "game_time",
    "time_of_day",
    "Park Factor"
]

# Filter down to required columns only
weather = weather[weather_cols]

# Merge on home_team + away_team
enriched = pd.merge(
    batters,
    weather,
    on=["home_team", "away_team"],
    how="left"
)

# Overwrite original cleaned file
enriched.to_csv(BATTERS_FILE, index=False)
