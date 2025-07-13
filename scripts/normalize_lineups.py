
import pandas as pd

# Load files
lineups = pd.read_csv('data/raw/lineups.csv')
team_map = pd.read_csv('data/Data/team_name_map.csv')
batters_cleaned = pd.read_csv('data/cleaned/batters_normalized_cleaned.csv')

# --- Normalize team name ---
team_map_dict = dict(zip(team_map['team'], team_map['name']))
lineups['name'] = lineups['name'].map(team_map_dict).fillna(lineups['name'])

# --- Normalize player names ---
def format_name(full_name):
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return full_name.strip()

lineups['last_name, first_name'] = lineups['last_name, first_name'].apply(format_name)

# Match only names that exist in cleaned batters list
valid_names = set(batters_cleaned['last_name, first_name'])
lineups = lineups[lineups['last_name, first_name'].isin(valid_names)]

# Save result
lineups.to_csv('data/raw/lineups_normalized.csv', index=False)
