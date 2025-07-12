
import pandas as pd

# Load CSVs
games = pd.read_csv("data/raw/todaysgames.csv")
lineups = pd.read_csv("data/raw/lineups.csv")
stadiums = pd.read_csv("data/Data/stadium_metadata.csv")
name_map = pd.read_csv("data/Data/team_name_map.csv")

# Normalize team names in games and lineups using name_map
name_dict = dict(zip(name_map["name"], name_map["team"]))

# Apply name normalization
games["home_team_normalized"] = games["home_team"].map(name_dict)
games["away_team_normalized"] = games["away_team"].map(name_dict)
lineups["team_normalized"] = lineups["team code"].map(name_dict)

# Merge stadium info using normalized home team
merged = games.merge(stadiums, left_on="home_team_normalized", right_on="home_team", how="left")

# Output merged data to file
merged.to_csv("data/processed/games_today.csv", index=False)
print("✅ games_today.csv created successfully.")
