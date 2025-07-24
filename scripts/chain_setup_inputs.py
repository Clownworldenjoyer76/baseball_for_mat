# chain_setup_inputs.py
import shutil
from pathlib import Path
import logging
from datetime import datetime
import sys

# Setup logging
log_dir = Path("summaries")
log_dir.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = log_dir / f"chain_setup_inputs_{timestamp}.log" # Log file name updated

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)
logging.getLogger().setLevel(logging.INFO)

def copy_files_to_end_chain():
    """
    Copies specified input files to the 'data/end_chain' directory.
    """
    input_files = [
        "data/adjusted/batters_home_weather_park.csv",
        "data/adjusted/batters_away_weather_park.csv",
        "data/adjusted/pitchers_home_weather_park.csv",
        "data/adjusted/pitchers_away_weather_park.csv",
        "data/raw/todaysgames_normalized.csv"
    ]

    output_dir = Path("data/end_chain")
    output_dir.mkdir(parents=True, exist_ok=True) # Ensure the output directory exists

    logging.info(f"Starting to copy input files to {output_dir}")

    for file_path_str in input_files:
        source_path = Path(file_path_str)
        destination_path = output_dir / source_path.name

        if not source_path.is_file():
            logging.error(f"❌ Source file not found: {source_path}")
            sys.exit(1) # Exit if a source file is missing

        try:
            shutil.copy2(source_path, destination_path)
            logging.info(f"✅ Copied '{source_path}' to '{destination_path}'")
        except Exception as e:
            logging.error(f"❌ Error copying '{source_path}' to '{destination_path}': {e}")
            sys.exit(1) # Exit on any copy error

    logging.info("🌟 All specified input files copied successfully to 'data/end_chain'.")

if __name__ == "__main__":
    try:
        copy_files_to_end_chain()
    except Exception as e:
        logging.error(f"❌ Script failed during execution: {e}")
        sys.exit(1)
