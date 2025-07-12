from pathlib import Path
import pandas as pd
from datetime import datetime
import os

# Load input files
pitchers_path = 'data/daily/todays_pitchers.csv'
team_map_path = 'data/Data/team_name_map.csv'
stadium_meta_path = 'data/Data/stadium_metadata.csv'

# Check if all required files exist
for path in [pitchers_path, team_map_path, stadium_meta_path]:
    if not Path(path).exists():
        raise FileNotFoundError(f"Required file not found: {path}")

# Load CSVs
pitchers_df = pd.read_csv(pitchers_path)
team_map_df = pd.read_csv(team_map_path)
stadium_df = pd.read_csv(stadium_meta_path)

# Validate required columns
required_pitcher_cols = ['away_team', 'away_pitcher', 'home_team', 'home_pitcher', 'game_time']
required_team_map_cols = ['name', 'team']
required_stadium_cols = ['home_team', 'venue', 'city', 'state', 'timezone', 'is_dome', 'latitude', 'longitude', 'game_time']

for col in required_pitcher_cols:
    if col not in pitchers_df.columns:
        raise KeyError(f"Missing column in todays_pitchers.csv: {col}")
for col in required_team_map_cols:
    if col not in team_map_df.columns:
        raise KeyError(f"Missing column in team_name_map.csv: {col}")
for col in required_stadium_cols:
    if col not in stadium_df.columns:
        raise KeyError(f"Missing column in stadium_metadata.csv: {col}")

# Merge team name mappings
pitchers_df['home_team'] = pitchers_df['home_team'].map(
    dict(zip(team_map_df['name'], team_map_df['team']))
)
pitchers_df['away_team'] = pitchers_df['away_team'].map(
    dict(zip(team_map_df['name'], team_map_df['team']))
)

# Merge stadium metadata
merged_df = pd.merge(pitchers_df, stadium_df, how='left', left_on='home_team', right_on='home_team')

# Output path
output_dir = Path('data/daily')
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / 'games_today.csv'

# Save to CSV
merged_df.to_csv(output_file, index=False)
print(f"✅ games_today.csv created successfully at {output_file}")
