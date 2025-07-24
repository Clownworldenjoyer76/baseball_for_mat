# scripts/chain_load_data.py

# Purpose: Responsible solely for loading raw data from its sources 
# (CSV, database, API, etc.) into pandas DataFrames. This script acts 
# as the initial ingestion point.

import pandas as pd
import os
from typing import Dict

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
            dataframes[name] = df
            print(f"Successfully loaded {name}. Shape: {df.shape}")
        except FileNotFoundError:
            print(f"ERROR: File not found at {path}. Please check the path.")
            raise
    print("--- Data Loading Process Complete ---")
    return dataframes

if __name__ == '__main__':
    files_to_load = {
        'games': 'data/end_chain/todaysgames_normalized.csv',
        'batters_home': 'data/end_chain/batters_home_weather_park.csv',
        'batters_away': 'data/end_chain/batters_away_weather_park.csv',
        'pitchers_home': 'data/end_chain/pitchers_home_weather_park.csv',
        'pitchers_away': 'data/end_chain/pitchers_away_weather_park.csv'
    }

    try:
        raw_dataframes = load_raw_data(files_to_load)

        print("\n--- Verifying Loaded Data (First 2 Rows) ---")
        for name, df in raw_dataframes.items():
            print(f"\nDataFrame: '{name}'")
            print(df.head(2))
            
    except FileNotFoundError:
        print("\nExecution stopped due to a missing file.")
