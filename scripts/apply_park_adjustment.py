import pandas as pd
import os
from datetime import datetime

# Load inputs
batters = pd.read_csv('data/batters_normalized_cleaned.csv')
games = pd.read_csv('data/raw/todaysgames_normalized.csv')
day_factors = pd.read_csv('data/Data/park_factors_day.csv')
night_factors = pd.read_csv('data/Data/park_factors_night.csv')

# Determine game time (hour)
games["hour"] = pd.to_datetime(games["game_time"]).dt.hour
games["time_of_day"] = games["hour"].apply(lambda x: "day" if x < 18 else "night")

# Create park factor mapping
day_map = day_factors.set_index("home_team")["Park Factor"].to_dict()
night_map = night_factors.set_index("home_team")["Park Factor"].to_dict()

# Merge park factor into games
games["park_factor"] = games.apply(lambda row: day_map.get(row["home_team"]) if row["time_of_day"] == "day" else night_map.get(row["home_team"]), axis=1)

# Create output directories if not exist
os.makedirs("data/adjusted", exist_ok=True)

# Apply park factor to batters based on opponent stadium
home_batters = batters[batters["team"] == "home"].copy()
away_batters = batters[batters["team"] == "away"].copy()

home_batters = home_batters.merge(games[["home_team", "park_factor"]], left_on="opponent", right_on="home_team", how="left")
away_batters = away_batters.merge(games[["home_team", "park_factor"]], left_on="opponent", right_on="home_team", how="left")

home_batters["adj_woba_park"] = home_batters["woba"] * home_batters["park_factor"]
away_batters["adj_woba_park"] = away_batters["woba"] * away_batters["park_factor"]

home_batters.to_csv("data/adjusted/batters_home_park.csv", index=False)
away_batters.to_csv("data/adjusted/batters_away_park.csv", index=False)

with open("log_park_home.txt", "w") as f:
    f.write("Home park factor applied to {} batters.\n".format(len(home_batters)))

with open("log_park_away.txt", "w") as f:
    f.write("Away park factor applied to {} batters.\n".format(len(away_batters)))
