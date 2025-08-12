import pandas as pd

# Define file paths
input_file = 'data/raw/mlb_schedule_today.csv'
output_file = 'data/bets/mlb_sched.csv'

# Read the input CSV file
try:
    df = pd.read_csv(input_file)
except FileNotFoundError:
    print(f"Error: The input file '{input_file}' was not found.")
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
]].copy() # Use .copy() to avoid SettingWithCopyWarning

# Rename the 'game_datetime' column to 'date'
df_cleaned.rename(columns={'game_datetime': 'date'}, inplace=True)

# Extract only the date portion from the 'date' column
df_cleaned['date'] = df_cleaned['date'].str.split('T').str[0]

# Create the output directory if it doesn't exist
import os
output_dir = os.path.dirname(output_file)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save the cleaned DataFrame to the output CSV file
df_cleaned.to_csv(output_file, index=False)

print(f"Data successfully cleaned and saved to '{output_file}'")
