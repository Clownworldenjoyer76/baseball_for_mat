import pandas as pd

# Load data
lineups_df = pd.read_csv('data/raw/lineups.csv')
games_df = pd.read_csv('data/raw/todaysgames.csv')
stadiums_df = pd.read_csv('data/stadium_metadata.csv')
batters_df = pd.read_csv('data/cleaned/batters_normalized_cleaned.csv')
pitchers_df = pd.read_csv('data/cleaned/pitchers_normalized_cleaned.csv')
team_map = pd.read_csv('data/Data/team_name_map.csv')

# Ensure required columns exist
if 'name' not in batters_df.columns:
    raise KeyError("Missing 'name' column in batters data.")
if 'name' not in pitchers_df.columns:
    raise KeyError("Missing 'name' column in pitchers data.")

# Format player names into last and first names
def format_name(name):
    if isinstance(name, str) and ',' in name:
        parts = name.split(',')
        return parts[0].strip(), parts[1].strip()
    return "", ""

batters_df[['last_name', 'first_name']] = pd.DataFrame(batters_df['name'].apply(format_name).tolist(), index=batters_df.index)
pitchers_df[['last_name', 'first_name']] = pd.DataFrame(pitchers_df['name'].apply(format_name).tolist(), index=pitchers_df.index)

# Team map validation
if 'name' not in team_map.columns or 'team' not in team_map.columns:
    raise KeyError("team_name_map.csv must have 'name' and 'team' columns")

# Example team code mapping (you'd have logic here to map games_df team names using team_map)

# Check stadiums_df is not empty before indexing
if stadiums_df.empty:
    raise ValueError("Stadium metadata is empty or filtered result is empty")

# Example access to first venue info after verifying non-empty
venue_info = stadiums_df.iloc[0]

# Dummy print to verify script works
print("generate_gamestoday.py ran successfully.")
