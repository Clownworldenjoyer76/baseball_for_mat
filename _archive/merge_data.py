import pandas as pd
from pathlib import Path
import sys
import logging
from datetime import datetime

# Set up persistent log directory
log_dir = Path("summaries")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"merge_script_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

def validate_columns(df, required_columns, df_name):
    logging.info(f"🔍 Validating columns for {df_name}...")
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logging.error(f"❌ {df_name} is missing required columns: {missing}")
        raise ValueError(f"{df_name} is missing required columns: {missing}")
    logging.info(f"✅ All required columns present in {df_name}.")

def main():
    logging.info("🔗 Starting merge process...")

    input_dir = Path("data/end_chain/cleaned")
    output_dir = Path("data/end_chain/final")
    output_dir.mkdir(parents=True, exist_ok=True)

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
            logging.error(f"❌ File not found: {path}")
            sys.exit(1)
        except pd.errors.EmptyDataError:
            logging.error(f"❌ File is empty: {path}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ Failed to load {name}: {e}")
            sys.exit(1)

    # Extract data
    batters_home = dataframes["batters_home"]
    batters_away = dataframes["batters_away"]
    pitchers_home = dataframes["pitchers_home"]
    pitchers_away = dataframes["pitchers_away"]
    games = dataframes["games"]

    # Validate required columns
    try:
        validate_columns(games, ['home_team', 'away_team', 'pitcher_home', 'pitcher_away'], 'games')
        validate_columns(batters_home, ['team'], 'batters_home')
        validate_columns(batters_away, ['team'], 'batters_away')
        validate_columns(pitchers_home, ['team'], 'pitchers_home')
        validate_columns(pitchers_away, ['team'], 'pitchers_away')
    except Exception as e:
        logging.critical(f"❌ Column validation failed: {e}")
        sys.exit(1)

    # Merge
    logging.info("🔄 Merging with batters_home...")
    merged_home = pd.merge(games, batters_home, left_on='home_team', right_on='team', how='left', suffixes=('', '_batter_home'))

    logging.info("🔄 Merging with batters_away...")
    merged = pd.merge(merged_home, batters_away, left_on='away_team', right_on='team', how='left', suffixes=('', '_batter_away'))

    logging.info("🔄 Merging with pitchers_home...")
    merged = pd.merge(merged, pitchers_home, left_on='pitcher_home', right_on='team', how='left', suffixes=('', '_pitcher_home'))

    logging.info("🔄 Merging with pitchers_away...")
    merged = pd.merge(merged, pitchers_away, left_on='pitcher_away', right_on='team', how='left', suffixes=('', '_pitcher_away'))

    output_path = output_dir / "combined_game_data.csv"

    # 🔧 Force overwrite: delete existing file
    if output_path.exists():
        output_path.unlink()
        logging.info(f"🧹 Removed existing file: {output_path}")

    try:
        merged.to_csv(output_path, index=False)
        logging.info(f"✅ Merged data saved to {output_path}")
        logging.info(f"📍 Absolute path: {output_path.resolve()}")
    except Exception as e:
        logging.critical(f"❌ Failed to write output: {e}")
        sys.exit(1)

    logging.info("🎉 Merge process completed.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"❌ Script failed: {e}", exc_info=True)
        sys.exit(1)
