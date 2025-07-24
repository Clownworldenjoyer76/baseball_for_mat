# scripts/clean_and_preprocess_games.py

import pandas as pd
from pathlib import Path
import os

def clean_and_preprocess_games(input_path: str, output_path: str) -> None:
    """
    Cleans and preprocesses the raw games data.

    Args:
        input_path (str): Path to the raw games CSV file.
        output_path (str): Path where the cleaned CSV will be saved.
    """
    print("🔧 Cleaning and preprocessing game-level data...")

    df = pd.read_csv(input_path)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Ensure required columns exist
    required_cols = ['home_team', 'away_team', 'pitcher_home', 'pitcher_away']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Standardize team names (strip/normalize spacing)
    df['home_team'] = df['home_team'].astype(str).str.strip()
    df['away_team'] = df['away_team'].astype(str).str.strip()

    # Standardize pitcher names (remove extra spacing)
    df['pitcher_home'] = df['pitcher_home'].astype(str).str.strip()
    df['pitcher_away'] = df['pitcher_away'].astype(str).str.strip()

    # Parse and standardize game_time if it exists
    if 'game_time' in df.columns:
        df['game_time'] = pd.to_datetime(df['game_time'], errors='coerce')
        if df['game_time'].isnull().any():
            print("⚠️ Warning: Some game_time values could not be parsed and were set to NaT.")

    # Fill NA values with explicit placeholders if needed
    df.fillna({'pitcher_home': 'Undecided', 'pitcher_away': 'Undecided'}, inplace=True)

    # Create output directory if it doesn't exist
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)
    print(f"✅ Saved cleaned game data to: {output_path}")

if __name__ == "__main__":
    INPUT_FILE = "data/end_chain/todaysgames_normalized.csv"
    OUTPUT_FILE = "data/end_chain/cleaned/games_cleaned.csv"

    clean_and_preprocess_games(INPUT_FILE, OUTPUT_FILE)
