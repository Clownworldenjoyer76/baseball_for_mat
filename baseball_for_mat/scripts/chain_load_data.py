# scripts/chain_load_data.py

import pandas as pd
import os
from typing import Dict

def load_raw_data(file_paths: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    Loads multiple raw CSV files into pandas DataFrames.

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
            print(f"✅ Loaded {name}. Shape: {df.shape}")
        except FileNotFoundError:
            print(f"❌ ERROR: File not found at {path}.")
            raise
    print("--- Data Loading Complete ---")
    return dataframes

if __name__ == '__main__':
    files_to_load = {
        'games': 'data/end_chain/first/games_today.csv',
        'batters_home': 'data/end_chain/first/raw/bat_hwp_dirty.csv',
        'batters_away': 'data/end_chain/first/raw/bat_awp_dirty.csv',
        'pitchers_home': 'data/end_chain/first/pit_hwp.csv',
        'pitchers_away': 'data/end_chain/first/pit_awp.csv'
    }

    try:
        raw_dataframes = load_raw_data(files_to_load)
    except FileNotFoundError:
        print("\n⛔ Execution stopped due to missing file(s).")
