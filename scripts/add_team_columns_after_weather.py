import pandas as pd

# Input files
weather_file = "data/weather_adjustments.csv"
stadium_file = "data/Data/stadium_metadata.csv"

# Load weather adjustments and stadium metadata
weather_df = pd.read_csv(weather_file)
stadium_df = pd.read_csv(stadium_file)

# Map stadium to away team from metadata
stadium_map = stadium_df.set_index("stadium")["away_team"].to_dict()

# Add columns
weather_df["home_team"] = weather_df["stadium"]
weather_df["away_team"] = weather_df["stadium"].map(stadium_map)

# Save updated weather_adjustments.csv
weather_df.to_csv(weather_file, index=False)
print(f"✅ Updated {weather_file} with home_team and away_team columns.")
