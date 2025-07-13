import pandas as pd

# Load files
lineups = pd.read_csv('data/raw/lineups.csv')
team_map = pd.read_csv('data/Data/team_name_map.csv')
batters_cleaned = pd.read_csv('data/cleaned/batters_normalized_cleaned.csv')

# --- Map team_code to full team_name ---
lineups['team_code'] = lineups['team_code'].str.strip().str.upper()
team_map_dict = dict(zip(team_map['team'].str.strip().str.upper(), team_map['name'].str.strip()))
lineups['team_name'] = lineups['team_code'].map(team_map_dict).fillna(lineups['team_code'])

# DEBUG: Print a few rows of mapped names to confirm mapping worked
print("TEAM MAPPING SAMPLE:")
print(lineups[['team_code', 'team_name']].drop_duplicates().head(10))

# --- Normalize player names ---
def format_name(full_name):
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return full_name.strip()

lineups['last_name, first_name'] = lineups['last_name, first_name'].apply(format_name)

# Filter to only valid names
valid_names = set(batters_cleaned['last_name, first_name'])
lineups = lineups[lineups['last_name, first_name'].isin(valid_names)]

# Save result
lineups.to_csv('data/raw/lineups_normalized.csv', index=False)
