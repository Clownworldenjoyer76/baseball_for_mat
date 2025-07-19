
import pandas as pd

# Load files
games = pd.read_csv('data/raw/todaysgames.csv')
team_map = pd.read_csv('data/Data/team_abv_map.csv')
pitchers = pd.read_csv('data/cleaned/pitchers_normalized_cleaned.csv')

# Normalize team names
team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

# Normalize pitcher names
def normalize_name(name):
    parts = name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name.strip()

games['pitcher_home'] = games['pitcher_home'].apply(normalize_name)
games['pitcher_away'] = games['pitcher_away'].apply(normalize_name)

# Allow pitchers to be in valid list OR equal to 'Undecided'
valid_pitchers = set(pitchers['last_name, first_name'])
games = games[
    (games['pitcher_home'].isin(valid_pitchers) | (games['pitcher_home'] == 'Undecided')) &
    (games['pitcher_away'].isin(valid_pitchers) | (games['pitcher_away'] == 'Undecided'))
]

# Save normalized file
games.to_csv('data/raw/todaysgames_normalized.csv', index=False)

print("normalize_todays_games.py completed successfully. Output saved to data/raw/todaysgames_normalized.csv")
