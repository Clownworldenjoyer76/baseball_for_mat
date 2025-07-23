import pandas as pd

# Input files
weather_file = "data/weather_adjustments.csv"
stadium_file = "data/Data/stadium_metadata.csv"

# Load data
weather_df = pd.read_csv(weather_file)
stadium_df = pd.read_csv(stadium_file)

# Force column: home_team = stadium
if "stadium" in weather_df.columns:
    weather_df["home_team"] = weather_df["stadium"]
else:
    weather_df["home_team"] = "UNKNOWN"

# Build mapping: venue -> away_team
if "venue" in stadium_df.columns and "away_team" in stadium_df.columns:
    stadium_map = stadium_df.set_index("venue")["away_team"].to_dict()
else:
    stadium_map = {}

# Map away_team using stadium → away_team
weather_df["away_team"] = weather_df.get("stadium", "").map(stadium_map).fillna("UNKNOWN")

# Overwrite file
weather_df.to_csv(weather_file, index=False)
print(f"✅ Forced update: {weather_file} now includes 'home_team' and 'away_team'.")
