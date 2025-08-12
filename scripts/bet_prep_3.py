# bet_prep_3.py

import pandas as pd
from datetime import datetime
import os

# Define file paths
sched_file = 'data/bets/mlb_sched.csv'
pitcher_props_file = 'data/_projections/pitcher_mega_z.csv'
output_dir = 'data/bets/prep'
output_file = os.path.join(output_dir, 'pitcher_props_bets.csv')

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Load the input files
try:
    mlb_sched_df = pd.read_csv(sched_file)
    pitcher_df = pd.read_csv(pitcher_props_file)
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure input files are in the correct directory.")
    exit()

# --- Merge date and game_id from schedule ---

# Prepare mlb_sched_df for merging by creating a single 'team' column
mlb_sched_away = mlb_sched_df.rename(columns={'away_team': 'team'})
mlb_sched_home = mlb_sched_df.rename(columns={'home_team': 'team'})
mlb_sched_merged = pd.concat([mlb_sched_away, mlb_sched_home])

# Merge pitcher_df with the prepared schedule data to add 'date' and 'game_id'
# A left merge keeps all rows from the original pitcher_df
pitcher_df = pd.merge(
    pitcher_df,
    mlb_sched_merged[['team', 'date', 'game_id']],
    on='team',
    how='left'
)

# --- Add, Rename, and Modify Columns ---

# Copy "name" column to a new "player" column
pitcher_df['player'] = pitcher_df['name']

# Rename "prop_type" to "prop"
if 'prop_type' in pitcher_df.columns:
    pitcher_df.rename(columns={'prop_type': 'prop'}, inplace=True)
else:
    pitcher_df['prop'] = '' # Create column if it doesn't exist

# Create new columns with static or empty values
pitcher_df['player_pos'] = 'pitcher'
pitcher_df['sport'] = 'baseball'
pitcher_df['league'] = 'MLB'
pitcher_df['timestamp'] = datetime.now().isoformat()

# Create empty columns if they don't already exist
for col in ['bet_type', 'prop_correct', 'book', 'price']:
    if col not in pitcher_df.columns:
        pitcher_df[col] = ''

# Convert 'game_id' to string to handle potential merge-related issues (like NaN)
if 'game_id' in pitcher_df.columns:
    pitcher_df['game_id'] = pitcher_df['game_id'].astype(str)


# --- Save the Final Output ---

pitcher_df.to_csv(output_file, index=False)

print(f"Script bet_prep_3.py completed successfully.")
print(f"Output file created at: {output_file}")
