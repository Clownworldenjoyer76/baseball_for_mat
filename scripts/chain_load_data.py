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
    END_CHAIN_DIR = 'data/end_chain'

    files_to_load = {
        'games': os.path.join(END_CHAIN_DIR, 'todaysgames_normalized.csv'),
        'batters_home': os.path.join(END_CHAIN_DIR, 'batters_home_weather_park.csv'),
        'batters_away': os.path.join(END_CHAIN_DIR, 'batters_away_weather_park.csv'),
        'pitchers_home': os.path.join(END_CHAIN_DIR, 'pitchers_home_weather_park.csv'),
        'pitchers_away': os.path.join(END_CHAIN_DIR, 'pitchers_away_weather_park.csv')
    }

    try:
        raw_dataframes = load_raw_data(files_to_load)

        # Optional verification
        print("\n--- Preview: First 2 Rows of Each DataFrame ---")
        for name, df in raw_dataframes.items():
            print(f"\n📄 {name}")
            print(df.head(2))

    except FileNotFoundError:
        print("\n⛔ Execution stopped due to missing file(s).")
