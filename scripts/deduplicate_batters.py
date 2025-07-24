# scripts/deduplicate_batters.py

import pandas as pd
from pathlib import Path

def clean_and_deduplicate(file_path: Path) -> None:
    df = pd.read_csv(file_path)

    # Drop columns ending in '_y'
    df = df.loc[:, ~df.columns.str.endswith('_y')]

    # Remove duplicate rows based on 'last_name, first_name'
    if 'last_name, first_name' in df.columns:
        df = df.drop_duplicates(subset='last_name, first_name', keep='first')

    # Save cleaned file
    df.to_csv(file_path, index=False)
    print(f"✅ Cleaned and deduplicated: {file_path.name} → {df.shape[0]} rows")

if __name__ == "__main__":
    input_files = [
        Path("data/end_chain/batters_home_weather_park.csv"),
        Path("data/end_chain/batters_away_weather_park.csv"),
    ]

    for path in input_files:
        if path.exists():
            clean_and_deduplicate(path)
        else:
            print(f"❌ File not found: {path}")
