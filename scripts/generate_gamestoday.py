
import pandas as pd
import numpy as np

# Load stadium metadata (corrected path)
stadiums_df = pd.read_csv('data/Data/stadium_metadata.csv')

# Load normalized and cleaned batter and pitcher data
batters_df = pd.read_csv('data/cleaned/batters_normalized_cleaned.csv')
pitchers_df = pd.read_csv('data/cleaned/pitchers_normalized_cleaned.csv')

# Load today's game schedule and lineups
games_df = pd.read_csv('data/raw/todaysgames.csv')
lineups_df = pd.read_csv('data/raw/lineups.csv')

# Load team name mapping
team_map = pd.read_csv('data/Data/team_name_map.csv')
team_map_dict = dict(zip(team_map['name'], team_map['team']))

# Standardize team names
games_df['away_team'] = games_df['away_team'].map(team_map_dict)
games_df['home_team'] = games_df['home_team'].map(team_map_dict)

# Format name to Last, First
def format_name(name):
    if pd.isnull(name):
        return ''
    parts = name.split()
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

batters_df["name"] = batters_df["name"].apply(format_name)
pitchers_df["name"] = pitchers_df["name"].apply(format_name)

# Dummy print for success confirmation
print("All data successfully loaded and processed.")
