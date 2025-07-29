# scripts/pit2.py

import pandas as pd
from pathlib import Path
import logging

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- File Paths ---
STARTING_PITCHERS_PATH = Path("data/end_chain/final/startingpitchers.csv")
PITCHERS_REFERENCE_PATH = Path("data/pitchers.csv")
OUTPUT_PATH = Path("data/end_chain/final/startingpitchers.csv")  # Overwrite same file

def main():
    logger.info("📥 Loading input files...")

    if not STARTING_PITCHERS_PATH.exists():
        logger.error(f"Missing starting pitcher file: {STARTING_PITCHERS_PATH}")
        return

    if not PITCHERS_REFERENCE_PATH.exists():
        logger.error(f"Missing pitcher reference file: {PITCHERS_REFERENCE_PATH}")
        return

    # Load data
    starting = pd.read_csv(STARTING_PITCHERS_PATH)
    pitchers_ref = pd.read_csv(PITCHERS_REFERENCE_PATH)

    if 'name' not in pitchers_ref.columns or 'throws' not in pitchers_ref.columns:
        logger.error("Missing 'name' or 'throws' column in reference file.")
        return

    # Drop duplicates in reference data
    pitchers_ref = pitchers_ref[['name', 'throws']].drop_duplicates()

    # Merge 'throws' into starting pitchers
    updated = starting.merge(pitchers_ref, on='name', how='left')

    missing_throws = updated['throws'].isnull().sum()
    if missing_throws > 0:
        logger.warning(f"⚠️ {missing_throws} pitchers are missing throwing hand data after merge.")

    # Save to the same file (overwrite)
    updated.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"✅ Updated starting pitchers file saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
