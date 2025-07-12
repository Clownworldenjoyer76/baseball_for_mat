
import pandas as pd

# Load data with correct paths
stadiums_df = pd.read_csv('data/Data/stadium_metadata.csv')
todays_games_df = pd.read_csv('data/raw/todaysgames.csv')
lineups_df = pd.read_csv('data/raw/lineups.csv')
batters_df = pd.read_csv('data/cleaned/batters_normalized_cleaned.csv')
pitchers_df = pd.read_csv('data/cleaned/pitchers_normalized_cleaned.csv')
team_map_df = pd.read_csv('data/Data/team_name_map.csv')

# Confirm required columns exist
required_batter_cols = ['name']
missing = [col for col in required_batter_cols if col not in batters_df.columns]
if missing:
    raise KeyError(f"Missing columns in batter file: {missing}")

# Clean names
def format_name(name):
    if pd.isna(name):
        return ''
    parts = str(name).split()
    if len(parts) == 1:
        return f"{parts[0]},"
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

batters_df["name"] = batters_df["name"].apply(format_name)

# Example dummy merge using team map (custom logic will be needed in full script)
if "name" not in team_map_df.columns or "team" not in team_map_df.columns:
    raise KeyError("Missing 'name' or 'team' in team_name_map.csv")

# Simulate main output (e.g., generate dummy merged DataFrame)
output = todays_games_df.copy()
output["home_stadium"] = output["home_team"].map(
    dict(zip(team_map_df["name"], team_map_df["team"]))
)

# Output path
output.to_csv("data/output/merged_games.csv", index=False)
