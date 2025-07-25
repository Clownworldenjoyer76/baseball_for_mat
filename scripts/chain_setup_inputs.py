import shutil
from pathlib import Path
import logging
from datetime import datetime
import sys

# Setup logging
# log_dir = Path("summaries") # No longer needed if not writing to a file
# log_dir.mkdir(parents=True, exist_ok=True) # No longer needed
# log_path = log_dir / f"chain_setup_inputs_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log" # No longer needed

logging.basicConfig(
    # Removed filename=log_path, so it will only log to handlers added manually
    level=logging.INFO,
    format="%(message)s"
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(console)
# Set the root logger level again to ensure console output respects INFO
logging.getLogger().setLevel(logging.INFO)


def copy_files_to_end_chain():
    input_files = [
        "data/adjusted/batters_home_weather_park.csv",
        "data/adjusted/batters_away_weather_park.csv",
        "data/adjusted/pitchers_home_weather_park.csv",
        "data/adjusted/pitchers_away_weather_park.csv",
        "data/raw/todaysgames_normalized.csv"
    ]

    output_dir = Path("data/end_chain")
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Copying input files...")

    for file_path_str in input_files:
        src = Path(file_path_str)
        dst = output_dir / src.name

        if not src.is_file():
            logging.error(f"Missing: {src}")
            sys.exit(1)

        try:
            shutil.copy2(src, dst)
            logging.info(f"Copied: {src.name}")
        except Exception as e:
            logging.error(f"Copy failed: {src.name} → {dst.name} ({e})")
            sys.exit(1)

    logging.info("All files copied.")

if __name__ == "__main__":
    try:
        copy_files_to_end_chain()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)
