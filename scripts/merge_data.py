import pandas as pd
from pathlib import Path
import sys

def validate_columns(df, required_columns, df_name):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")

def main():
    print("🔗 Starting merge process...")

    input_dir = Path("data/end_chain/cleaned")
    output_dir = Path("data/end_chain/final")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load inputs
    try:
        batters_home = pd.read_csv(input_dir / "batters_home_cleaned.csv")
        batters_away = pd.read_csv(input_dir / "batters_away_cleaned.csv")
        pitchers_home = pd.read_csv(input_dir / "pitchers_home_cleaned.csv")
        pitchers_away = pd.read_csv(input_dir / "pitchers_away_cleaned.csv")
        games = pd.read_csv(input_dir / "games_cleaned.csv")
    except Exception as e:
        print(f"❌ Failed to load input files: {e}")
        sys.exit(1)

    print("✅ All input files loaded")

    # Validate merge keys
    try:
        validate_columns(games, ['home_team', 'away_team', 'pitcher_home', 'pitcher_away'], 'games')
        validate_columns(batters_home, ['team'], 'batters_home')
        validate_columns(batters_away, ['team'], 'batters_away')
        validate_columns(pitchers_home, ['team'], 'pitchers_home')
        validate_columns(pitchers_away, ['team'], 'pitchers_away')
    except Exception as e:
        print(f"❌ Column validation failed: {e}")
        sys.exit(1)

    print("✅ Required columns validated")

    # Merge batters
    merged_home = pd.merge(games, batters_home, left_on='home_team', right_on='team', how='left', suffixes=('', '_batter_home'))
    merged = pd.merge(merged_home, batters_away, left_on='away_team', right_on='team', how='left', suffixes=('', '_batter_away'))

    # Merge pitchers
    merged = pd.merge(merged, pitchers_home, left_on='pitcher_home', right_on='team', how='left', suffixes=('', '_pitcher_home'))
    merged = pd.merge(merged, pitchers_away, left_on='pitcher_away', right_on='team', how='left', suffixes=('', '_pitcher_away'))

    output_path = output_dir / "combined_game_data.csv"
    merged.to_csv(output_path, index=False)

    print(f"📦 Merged data saved to: {output_path}")
    print("✅ Merge complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Script failed during execution: {e}")
        sys.exit(1)
