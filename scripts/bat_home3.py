import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Mapping dictionary for known edge-case corrections
TEAM_CORRECTIONS = {
    "D-Backs": "D-backs",
    "La Angels": "Angels",
    "La Dodgers": "Dodgers",
    "Chi Cubs": "Cubs",
    "Chi White Sox": "White Sox",
    "Sf Giants": "Giants",
    "Ny Mets": "Mets",
    "Ny Yankees": "Yankees",
    "Bos Red Sox": "Red Sox",
    "Tor Blue Jays": "Blue Jays",
    "Tb Rays": "Rays",
    "Sd Padres": "Padres",
    "Stl Cardinals": "Cardinals",
    "Wsh Nationals": "Nationals",
    "KC Royals": "Royals"
    # Add more as needed
}

def normalize_team_name(name):
    if not isinstance(name, str):
        return name
    name = name.strip().title()
    return TEAM_CORRECTIONS.get(name, name)

def update_dataframe_from_source(base_df: pd.DataFrame, source_file_path: Path, columns_to_copy: list) -> pd.DataFrame:
    if not source_file_path.exists():
        logging.warning(f"Source file not found: {source_file_path}. Skipping update.")
        return base_df

    try:
        source_df = pd.read_csv(source_file_path)
    except pd.errors.EmptyDataError:
        logging.warning(f"Source file is empty: {source_file_path}. Skipping update.")
        return base_df
    except Exception as e:
        logging.error(f"Error reading source file {source_file_path}: {e}. Skipping update.")
        return base_df

    merge_key = 'home_team'
    if merge_key not in base_df.columns:
        logging.warning(f"'{merge_key}' column not found in base DataFrame. Skipping update.")
        return base_df

    if merge_key not in source_df.columns:
        logging.warning(f"'{merge_key}' column not found in {source_file_path}. Skipping update.")
        return base_df

    # Normalize home_team in both dataframes
    base_df[merge_key] = base_df[merge_key].apply(normalize_team_name)
    source_df[merge_key] = source_df[merge_key].apply(normalize_team_name)

    columns_to_update = [
        col for col in columns_to_copy
        if col in base_df.columns and col in source_df.columns
    ]

    if not columns_to_update:
        logging.info(f"No matching columns to update from {source_file_path}.")
        return base_df

    source_subset = source_df[[merge_key] + columns_to_update].drop_duplicates(subset=[merge_key])
    merged_df = base_df.merge(source_subset, on=merge_key, how='left', suffixes=('', '_source_new'))

    for col in columns_to_update:
        new_col = f"{col}_source_new"
        if new_col in merged_df.columns:
            base_df[col] = merged_df[new_col].combine_first(base_df[col])

    logging.info(f"Successfully updated data from {source_file_path}.")
    return base_df

def main():
    BASE_FILE = Path("data/end_chain/final/updating/bat_home2.csv")
    OUTPUT_FILE = Path("data/end_chain/final/updating/bat_home3.csv")

    source_configs = [
        (Path("data/end_chain/cleaned/games_today_cleaned.csv"), ['pitcher_away', 'pitcher_home']),
        (Path("data/Data/stadium_metadata.csv"), ['away_team', 'game_time', 'venue', 'Park Factor', 'timezone', 'time_of_day', 'state', 'latitude', 'longitude', 'city', 'is_dome']),
        (Path("data/weather_input.csv"), []),
        (Path("data/weather_adjustments.csv"), ['humidity', 'location', 'wind_direction', 'notes', 'precipitation', 'condition', 'temperature', 'wind_speed']),
        (Path("data/adjusted/batters_home_weather_park.csv"), ['adj_woba_combined', 'adj_woba_weather']),
    ]

    if not BASE_FILE.exists():
        logging.critical(f"Base file not found: {BASE_FILE}. Cannot proceed.")
        return

    try:
        df = pd.read_csv(BASE_FILE)
    except Exception as e:
        logging.critical(f"Error loading base file {BASE_FILE}: {e}")
        return

    logging.info(f"Loaded base DataFrame with {len(df)} rows.")

    for file_path, columns in source_configs:
        df = update_dataframe_from_source(df, file_path, columns)

    # Optional: remove duplicate rows
    df = df.drop_duplicates()

    try:
        df.to_csv(OUTPUT_FILE, index=False)
        logging.info(f"Final updated DataFrame saved to {OUTPUT_FILE}.")
    except Exception as e:
        logging.error(f"Failed to save output file: {e}")

if __name__ == "__main__":
    main()
