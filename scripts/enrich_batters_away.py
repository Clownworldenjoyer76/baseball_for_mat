import pandas as pd
from pathlib import Path

# Load input files
batters_path = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
weather_input_path = Path("data/weather_input.csv")
weather_adj_path = Path("data/weather_adjustments.csv")
output_path = Path("data/end_chain/cleaned/batters_away_enriched.csv")

# Read CSVs
batters = pd.read_csv(batters_path)
weather_input = pd.read_csv(weather_input_path)
weather_adj = pd.read_csv(weather_adj_path)

# Normalize team names and game_time
for df in [weather_input, weather_adj]:
    df["away_team"] = df["away_team"].str.strip().str.title()
    df["home_team"] = df["team_name_x"].str.strip().str.title()
    df["game_time"] = df["game_time"].str.strip()

# Merge in park and timing info
batters = pd.merge(
    batters,
    weather_input[[
        "away_team", "home_team", "game_time", "time_of_day", "Park Factor"
    ]],
    on="away_team",
    how="left"
)

# Merge in weather conditions
batters = pd.merge(
    batters,
    weather_adj[[
        "away_team", "game_time", "temperature", "wind_speed", "wind_direction", "humidity", "condition"
    ]],
    on=["away_team", "game_time"],
    how="left"
)

# Save enriched output
batters.to_csv(output_path, index=False)
print(f"✅ Saved: {output_path}")
