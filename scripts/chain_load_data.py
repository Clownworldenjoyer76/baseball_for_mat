# Chain_load_data.py

# Purpose: Responsible solely for loading raw data from its sources 
# (CSV, database, API, etc.) into pandas DataFrames. This script acts 
# as the initial ingestion point.

import pandas as pd
import os
from typing import Dict, List

def load_raw_data(file_paths: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    Loads multiple raw CSV files into pandas DataFrames.

    This function is the first step in the data pipeline, focusing only on
    ingesting data from specified file paths without performing any complex
    transformations.

    Args:
        file_paths (Dict[str, str]): A dictionary where keys are descriptive
                                     names (e.g., 'games', 'batters_home') and
                                     values are the string paths to the CSV files.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary containing the loaded DataFrames,
                                 keyed by the same names provided in the input.
                                 
    Raises:
        FileNotFoundError: If a file path provided does not exist.
    """
    dataframes = {}
    print("--- Starting Data Loading Process ---")
    for name, path in file_paths.items():
        try:
            print(f"Loading {name} from {path}...")
            df = pd.read_csv(path)
            # Example of an essential, immediate type conversion if needed:
            # if 'game_date' in df.columns:
            #     df['game_date'] = pd.to_datetime(df['game_date'])
            dataframes[name] = df
            print(f"Successfully loaded {name}. Shape: {df.shape}")
        except FileNotFoundError:
            print(f"ERROR: File not found at {path}. Please check the path.")
            raise
    print("--- Data Loading Process Complete ---")
    return dataframes

# This block demonstrates the script's usage and would typically not run when
# this module is imported by another script. It assumes data files are in a
# subdirectory named 'data'.
if __name__ == '__main__':
    # Define the relative paths to the raw data files.
    # It's good practice to place raw data in a dedicated directory.
    DATA_DIR = 'data'
    
    # Create dummy data files for demonstration purposes if they don't exist.
    if not os.path.exists(DATA_DIR):
        print(f"Creating dummy data directory: '{DATA_DIR}'")
        os.makedirs(DATA_DIR)
        
        # Dummy dataframes
        dummy_files = {
            'games': pd.DataFrame({'game_id': [1, 2], 'game_date': ['2025-07-23', '2025-07-24']}),
            'batters_home': pd.DataFrame({'game_id': [1, 2], 'player_id': [101, 102], 'at_bats': [4, 3]}),
            'batters_away': pd.DataFrame({'game_id': [1, 2], 'player_id': [201, 202], 'at_bats': [3, 5]}),
            'pitchers_home': pd.DataFrame({'game_id': [1, 2], 'player_id': [301, 302], 'innings_pitched': [6.0, 7.1]}),
            'pitchers_away': pd.DataFrame({'game_id': [1, 2], 'player_id': [401, 402], 'innings_pitched': [5.2, 8.0]})
        }
        
        for name, df in dummy_files.items():
            path = os.path.join(DATA_DIR, f"{name}.csv")
            if not os.path.exists(path):
                print(f"Creating dummy file: {path}")
                df.to_csv(path, index=False)

    # --- Script Execution ---
    
    # Define the dictionary of file paths.
    files_to_load = {
        'games': os.path.join(DATA_DIR, 'games.csv'),
        'batters_home': os.path.join(DATA_DIR, 'batters_home.csv'),
        'batters_away': os.path.join(DATA_DIR, 'batters_away.csv'),
        'pitchers_home': os.path.join(DATA_DIR, 'pitchers_home.csv'),
        'pitchers_away': os.path.join(DATA_DIR, 'pitchers_away.csv')
    }

    # Load the raw data.
    try:
        raw_dataframes = load_raw_data(files_to_load)

        # Output: The loaded DataFrames are now available in the `raw_dataframes` dictionary.
        # An orchestrator script would take this dictionary and pass it to the next
        # step in the pipeline (e.g., data cleaning).
        
        print("\n--- Verifying Loaded Data (First 2 Rows) ---")
        for name, df in raw_dataframes.items():
            print(f"\nDataFrame: '{name}'")
            print(df.head(2))
            
    except FileNotFoundError:
        print("\nExecution stopped due to a missing file.")

