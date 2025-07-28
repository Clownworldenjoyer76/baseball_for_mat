# scripts/bat_away3.py

import pandas as pd
from pathlib import Path

# File paths
BASE_FILE = Path("data/end_chain/final/updating/bat_away2.csv")
STADIUM_FILE = Path("data/Data/stadium_metadata.csv")
GAMES_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
WEATHER_ADJ_FILE = Path("data/weather_adjustments.csv")
WEATHER_INPUT_FILE = Path("data/weather_input.csv")
OUTPUT_FILE = Path("data/end_chain/final/updating/bat_away3.csv")

# Load base dataframe
df = pd.read_csv(BASE_FILE)

# Define column mappings per source file
source_files = [
    (STADIUM_FILE, ['timezone', 'is_dome', 'game_time']),
    (GAMES_FILE, ['home_team']),
    (WEATHER_ADJ_FILE, ['condition', 'humidity', 'notes', 'precipitation', 'temperature',
                        'wind_direction', 'wind_speed', 'location']),
    (WEATHER_INPUT_FILE, ['Park Factor', 'city', 'latitude', 'longitude', 'state', 'time_of_day'])
]

# Merge updates one by one
for file_path, columns in source_files:
    if not file_path.exists():
        continue
    source_df = pd.read_csv(file_path)
    if 'away_team' not in source_df.columns:
        continue
    merge_cols = ['away_team']
    shared_cols = [col for col in columns if col in df.columns and col in source_df.columns]
    if shared_cols:
        merged = df.merge(source_df[merge_cols + shared_cols], on='away_team', how='left', suffixes=('', '_new'))
        for col in shared_cols:
            update_col = f"{col}_new"
            df[col] = merged[update_col].combine_first(df[col])
        df = df.drop(columns=[f"{col}_new" for col in shared_cols if f"{col}_new" in merged.columns])

# Save final output
df.to_csv(OUTPUT_FILE, index=False)
