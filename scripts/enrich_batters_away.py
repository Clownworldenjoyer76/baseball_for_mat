import pandas as pd
from pathlib import Path

# Load data
batters_path = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
weather_path = Path("data/weather_adjustments.csv")
stadium_path = Path("data/Data/stadium_metadata.csv")

batters = pd.read_csv(batters_path)
weather = pd.read_csv(weather_path)
stadium = pd.read_csv(stadium_path)

# Merge stadium info into weather (to get Park Factor + time_of_day)
weather = weather.merge(
    stadium[["home_team", "game_time", "Park Factor", "time_of_day"]],
    on=["home_team", "game_time"],
    how="left"
)

# Rename for consistency
weather.rename(columns={
    "temperature": "temp",
    "condition": "weather_condition"
}, inplace=True)

# Select columns to inject
weather_cols = [
    "away_team", "home_team", "game_time", "temp", "humidity", "wind_speed",
    "wind_direction", "weather_condition", "Park Factor", "time_of_day"
]

weather = weather[weather_cols]

# Merge into batters
batters = pd.merge(
    batters,
    weather,
    on=["away_team", "home_team", "game_time"],
    how="left"
)

# Save to same path (overwrite)
batters.to_csv(batters_path, index=False)
