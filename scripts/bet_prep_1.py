import pandas as pd
import os

# Define file paths
input_file = 'data/raw/mlb_schedule_today.csv'
mapping_file = 'data/Data/team_name_map.csv'
output_file = 'data/bets/mlb_sched.csv'

# Read the input CSV file
try:
    df = pd.read_csv(input_file)
except FileNotFoundError:
    print(f"Error: The input file '{input_file}' was not found.")
    exit()

# Read the mapping CSV file
try:
    df_map = pd.read_csv(mapping_file)
except FileNotFoundError:
    print(f"Error: The mapping file '{mapping_file}' was not found.")
    exit()

# Select the required columns
df_cleaned = df[[
    'game_id',
    'game_datetime',
    'home_team_name',
    'home_team_id',
    'away_team_name',
    'away_team_id',
    'venue_name'
]].copy()

# Rename the 'game_datetime' column to 'date'
df_cleaned.rename(columns={'game_datetime': 'date'}, inplace=True)

# Extract only the date portion from the 'date' column
df_cleaned['date'] = df_cleaned['date'].str.split('T').str[0]

# --- New Logic for mapping team names ---

# Prepare mapping DataFrame for case-insensitive merge
df_map['name_lower'] = df_map['name'].str.lower()
df_map.rename(columns={'team': 'mapped_team'}, inplace=True)

# Map home team names
df_cleaned['home_team_name_lower'] = df_cleaned['home_team_name'].str.lower()
df_cleaned = df_cleaned.merge(df_map[['name_lower', 'mapped_team']],
                              left_on='home_team_name_lower',
                              right_on='name_lower',
                              how='left')
df_cleaned['home_team'] = df_cleaned['mapped_team'].fillna(df_cleaned['home_team_name'])
df_cleaned.drop(columns=['home_team_name', 'home_team_name_lower', 'name_lower', 'mapped_team'], inplace=True)

# Map away team names
df_cleaned['away_team_name_lower'] = df_cleaned['away_team_name'].str.lower()
df_cleaned = df_cleaned.merge(df_map[['name_lower', 'mapped_team']],
                              left_on='away_team_name_lower',
                              right_on='name_lower',
                              how='left')
df_cleaned['away_team'] = df_cleaned['mapped_team'].fillna(df_cleaned['away_team_name'])
df_cleaned.drop(columns=['away_team_name', 'away_team_name_lower', 'name_lower', 'mapped_team'], inplace=True)

# Reorder columns to match the requested output structure
new_cols = [
    'game_id',
    'date',
    'home_team',
    'home_team_id',
    'away_team',
    'away_team_id',
    'venue_name'
]
df_cleaned = df_cleaned[new_cols]

# Create the output directory if it doesn't exist
output_dir = os.path.dirname(output_file)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save the cleaned DataFrame to the output CSV file
df_cleaned.to_csv(output_file, index=False)

print(f"Data successfully cleaned and saved to '{output_file}'")

