# scripts/clean_merge_files.py

import pandas as pd
import os

def clean_last_name_first_name_column(filepath: str) -> pd.DataFrame:
    """
    Cleans the 'last_name, first_name' column in the given file by removing
    trailing commas and spaces.

    Args:
        filepath (str): The path to the CSV file to be cleaned.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    df = pd.read_csv(filepath)

    if 'last_name, first_name' in df.columns:
        df['last_name, first_name'] = (
            df['last_name, first_name']
            .astype(str)
            .str.replace(r',$', '', regex=True)
            .str.strip()
        )
    else:
        print(f"⚠️ WARNING: 'last_name, first_name' column not found in {filepath}")

    return df

if __name__ == "__main__":
    input_files = {
        "pitchers_home": "data/end_chain/pitchers_home_weather_park.csv",
        "pitchers_away": "data/end_chain/pitchers_away_weather_park.csv"
    }

    for label, path in input_files.items():
        if os.path.exists(path):
            print(f"Cleaning file: {path}")
            cleaned_df = clean_last_name_first_name_column(path)
            cleaned_df.to_csv(path, index=False)
            print(f"✅ {label} cleaned and saved.")
        else:
            print(f"❌ File not found: {path}")
