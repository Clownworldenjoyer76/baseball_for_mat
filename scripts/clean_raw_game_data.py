import pandas as pd
from pathlib import Path

# Load raw data
raw_pitchers = pd.read_csv('data/raw/todays_pitchers_raw.csv')
raw_lineups = pd.read_csv('data/raw/starting_lineups_raw.csv')

# Normalize team names
team_map = pd.read_csv('data/Data/team_name_map.csv')
team_dict = dict(zip(team_map['input'], team_map['standard']))

for col in ['away_team', 'home_team', 'team', 'opponent_team']:
    if col in raw_pitchers.columns:
        raw_pitchers[col] = raw_pitchers[col].map(team_dict).fillna(raw_pitchers[col])
    if col in raw_lineups.columns:
        raw_lineups[col] = raw_lineups[col].map(team_dict).fillna(raw_lineups[col])

# Normalize names
def normalize_name(name):
    parts = name.replace('.', '').split()
    if len(parts) == 2:
        return f"{parts[1]}, {parts[0]}"
    return name  # fallback

for col in ['away_pitcher', 'home_pitcher']:
    raw_pitchers[col] = raw_pitchers[col].apply(normalize_name)

raw_lineups['batter_name'] = raw_lineups['batter_name'].apply(normalize_name)

# Save cleaned files
Path('data/daily').mkdir(parents=True, exist_ok=True)
raw_pitchers.to_csv('data/daily/todays_pitchers.csv', index=False)
raw_lineups.to_csv('data/daily/starting_lineups.csv', index=False)

print("✅ Cleaned game data saved to data/daily/")
