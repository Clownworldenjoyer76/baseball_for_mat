import pandas as pd
from pathlib import Path
import sys
import logging
from datetime import datetime

# Configure logging
log_file = f"merge_script_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ])

def validate_columns(df, required_columns, df_name):
    """
    Validates if all required columns are present in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        required_columns (list): A list of column names that are required.
        df_name (str): The name of the DataFrame for logging purposes.

    Raises:
        ValueError: If any required columns are missing.
    """
    logging.info(f"🔍 Validating columns for {df_name}...")
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logging.error(f"❌ {df_name} is missing required columns: {missing}")
        raise ValueError(f"{df_name} is missing required columns: {missing}")
    logging.info(f"✅ All required columns present in {df_name}.")

def main():
    """
    Main function to perform the data merging process.
    """
    logging.info("🔗 Starting merge process...")

    input_dir = Path("data/end_chain/cleaned")
    output_dir = Path("data/end_chain/final")

    logging.info(f"📂 Ensuring output directory exists: {output_dir}")
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"✅ Output directory ensured: {output_dir}")
    except OSError as e:
        logging.error(f"❌ Failed to create output directory {output_dir}: {e}")
        sys.exit(1)

    # Load inputs
    file_paths = {
        "batters_home": input_dir / "batters_home_cleaned.csv",
        "batters_away": input_dir / "batters_away_cleaned.csv",
        "pitchers_home": input_dir / "pitchers_home_cleaned.csv",
        "pitchers_away": input_dir / "pitchers_away_cleaned.csv",
        "games": input_dir / "games_cleaned.csv",
    }
    dataframes = {}

    for name, path in file_paths.items():
        logging.info(f"⏳ Loading {name} from {path}...")
        try:
            dataframes[name] = pd.read_csv(path)
            logging.info(f"✅ Loaded {name} with {len(dataframes[name])} rows and {len(dataframes[name].columns)} columns.")
        except FileNotFoundError:
            logging.error(f"❌ Error: File not found at {path}. Please ensure all input files exist.")
            sys.exit(1)
        except pd.errors.EmptyDataError:
            logging.error(f"❌ Error: {path} is empty. Cannot process empty files.")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ Failed to load {name} from {path}: {e}")
            sys.exit(1)

    batters_home = dataframes["batters_home"]
    batters_away = dataframes["batters_away"]
    pitchers_home = dataframes["pitchers_home"]
    pitchers_away = dataframes["pitchers_away"]
    games = dataframes["games"]

    logging.info("✅ All input files loaded successfully.")

    # Validate merge keys
    logging.info("🔍 Starting column validation for merge keys...")
    try:
        validate_columns(games, ['home_team', 'away_team', 'pitcher_home', 'pitcher_away'], 'games')
        validate_columns(batters_home, ['team'], 'batters_home')
        validate_columns(batters_away, ['team'], 'batters_away')
        validate_columns(pitchers_home, ['team'], 'pitchers_home')
        validate_columns(pitchers_away, ['team'], 'pitchers_away')
        logging.info("✅ All required columns for merging are present.")
    except ValueError as e:
        logging.critical(f"❌ Column validation failed: {e}. Exiting script.")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"❌ An unexpected error occurred during column validation: {e}. Exiting script.")
        sys.exit(1)

    # Merge batters
    logging.info("🔄 Merging games with batters_home data...")
    initial_games_rows = len(games)
    merged_home = pd.merge(games, batters_home, left_on='home_team', right_on='team', how='left', suffixes=('', '_batter_home'))
    logging.info(f"ℹ️ After merging with batters_home: {len(merged_home)} rows.")
    if initial_games_rows != len(merged_home):
        logging.warning("⚠️ Row count changed after merging with batters_home. This might indicate issues with merge keys or duplicates in batters_home.")

    logging.info("🔄 Merging with batters_away data...")
    merged = pd.merge(merged_home, batters_away, left_on='away_team', right_on='team', how='left', suffixes=('', '_batter_away'))
    logging.info(f"ℹ️ After merging with batters_away: {len(merged)} rows.")
    if len(merged_home) != len(merged):
        logging.warning("⚠️ Row count changed after merging with batters_away. This might indicate issues with merge keys or duplicates in batters_away.")

    # Merge pitchers
    logging.info("🔄 Merging with pitchers_home data...")
    merged = pd.merge(merged, pitchers_home, left_on='pitcher_home', right_on='team', how='left', suffixes=('', '_pitcher_home'))
    logging.info(f"ℹ️ After merging with pitchers_home: {len(merged)} rows.")
    if len(merged_home) != len(merged): # Comparing to the size after first merge to check for consistent row count
        logging.warning("⚠️ Row count changed after merging with pitchers_home. This might indicate issues with merge keys or duplicates in pitchers_home.")


    logging.info("🔄 Merging with pitchers_away data...")
    merged = pd.merge(merged, pitchers_away, left_on='pitcher_away', right_on='team', how='left', suffixes=('', '_pitcher_away'))
    logging.info(f"ℹ️ After merging with pitchers_away: {len(merged)} rows.")
    if len(merged_home) != len(merged):
        logging.warning("⚠️ Row count changed after merging with pitchers_away. This might indicate issues with merge keys or duplicates in pitchers_away.")


    output_path = output_dir / "combined_game_data.csv"
    logging.info(f"💾 Saving merged data to: {output_path}")
    try:
        merged.to_csv(output_path, index=False)
        logging.info(f"✅ Merged data with {len(merged)} rows and {len(merged.columns)} columns saved successfully to: {output_path}")
    except Exception as e:
        logging.critical(f"❌ Failed to save merged data to {output_path}: {e}")
        sys.exit(1)

    logging.info("🎉 Merge process completed successfully!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"❌ Script failed during execution: {e}", exc_info=True)
        sys.exit(1)

