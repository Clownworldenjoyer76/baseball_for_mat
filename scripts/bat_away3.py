# scripts/bat_away3.py

# bat_away3.py

import pandas as pd
from pathlib import Path
import logging

# Configure logging for better visibility into script execution
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_dataframe_from_source(base_df: pd.DataFrame, source_file_path: Path, columns_to_copy: list) -> pd.DataFrame:
    """
    Updates the base DataFrame with data from a source file, matching on 'away_team'.
    Only updates columns that already exist in the base_df and are in columns_to_copy.
    """
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

    if 'away_team' not in source_df.columns:
        logging.warning(f"'away_team' column not found in {source_file_path}. Skipping update.")
        return base_df

    # Identify columns that exist in BOTH the base_df and the source_df, and are in columns_to_copy
    # This ensures we don't add new columns to base_df
    columns_to_update = [
        col for col in columns_to_copy
        if col in base_df.columns and col in source_df.columns
    ]

    if not columns_to_update:
        logging.info(f"No matching columns to update from {source_file_path}.")
        return base_df

    # Select only the necessary columns from the source DataFrame before merging
    # This includes the merge key ('away_team') and the identified columns to update
    source_subset = source_df[['away_team'] + columns_to_update].drop_duplicates(subset=['away_team'])

    # Perform a left merge to bring in the new values.
    # We use a temporary suffix to distinguish merged columns, then use combine_first.
    merged_df = base_df.merge(source_subset, on='away_team', how='left', suffixes=('', '_source_new'))

    # Update only the existing columns in base_df using combine_first
    for col in columns_to_update:
        new_col_name = f"{col}_source_new"
        if new_col_name in merged_df.columns: # Check if the suffixed column was actually created
            base_df[col] = merged_df[new_col_name].combine_first(base_df[col])

    logging.info(f"Successfully updated data from {source_file_path}.")
    return base_df

def main():
    # Define file paths
    BASE_FILE = Path("data/end_chain/final/updating/bat_away2.csv")
    STADIUM_FILE = Path("data/Data/stadium_metadata.csv")
    GAMES_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
    WEATHER_ADJ_FILE = Path("data/weather_adjustments.csv")
    WEATHER_INPUT_FILE = Path("data/weather_input.csv")
    OUTPUT_FILE = Path("data/end_chain/final/updating/bat_away3.csv")

    # Define source files and their corresponding columns to be updated
    # The order here is important as per the requirement
    source_configs = [
        (STADIUM_FILE, ['timezone', 'is_dome', 'game_time']),
        (GAMES_FILE, ['home_team']),
        (WEATHER_ADJ_FILE, ['condition', 'humidity', 'notes', 'precipitation', 'temperature',
                            'wind_direction', 'wind_speed', 'location']),
        (WEATHER_INPUT_FILE, ['Park Factor', 'city', 'latitude', 'longitude', 'state', 'time_of_day'])
    ]

    # Load the base DataFrame
    if not BASE_FILE.exists():
        logging.critical(f"Base file not found: {BASE_FILE}. Script cannot proceed.")
        return
    try:
        df = pd.read_csv(BASE_FILE)
        if 'away_team' not in df.columns:
            logging.critical(f"'away_team' column not found in base file {BASE_FILE}. Script cannot proceed.")
            return
    except pd.errors.EmptyDataError:
        logging.critical(f"Base file is empty: {BASE_FILE}. Script cannot proceed.")
        return
    except Exception as e:
        logging.critical(f"Error reading base file {BASE_FILE}: {e}. Script cannot proceed.")
        return

    logging.info(f"Successfully loaded base DataFrame from {BASE_FILE}.")

    # Sequentially update the DataFrame from each source
    for file_path, columns_to_copy in source_configs:
        df = update_dataframe_from_source(df, file_path, columns_to_copy)

    # Save the final updated DataFrame
    try:
        df.to_csv(OUTPUT_FILE, index=False)
        logging.info(f"Final updated DataFrame saved to {OUTPUT_FILE}.")
    except Exception as e:
        logging.error(f"Error saving output file to {OUTPUT_FILE}: {e}")

if __name__ == "__main__":
    main()
