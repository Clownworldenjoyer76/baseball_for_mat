# chain_setup_inputs.py

import os
import shutil
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def setup_raw_data_inputs(source_dir='data', destination_dir='raw_data'):
    """
    Copies specified raw data files from a source directory to a
    designated raw_data input directory for the pipeline.

    Args:
        source_dir (str): The directory where the original raw data files are located.
        destination_dir (str): The directory where the copied raw data files will be placed.
                               This directory will be created/cleared if it exists.
    """
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - Starting chain_setup_inputs.py script.")

    # Define the names of the raw CSV files expected
    required_files = [
        'batters_home.csv',
        'batters_away.csv',
        'pitchers_home.csv',
        'pitchers_away.csv',
        'games.csv',
    ]

    # --- Prepare the destination directory ---
    if os.path.exists(destination_dir):
        logging.info(f"Clearing existing directory: {destination_dir}")
        shutil.rmtree(destination_dir) # Remove existing content
    
    os.makedirs(destination_dir, exist_ok=True) # Create it fresh
    logging.info(f"Prepared destination directory: {destination_dir}")

    # --- Copy files from source to destination ---
    all_files_found = True
    for filename in required_files:
        source_path = os.path.join(source_dir, filename)
        destination_path = os.path.join(destination_dir, filename)

        try:
            if not os.path.exists(source_path):
                logging.error(f"❌ Required file not found in source: {source_path}")
                all_files_found = False
                continue # Continue checking for other missing files

            shutil.copy2(source_path, destination_path) # copy2 preserves metadata
            logging.info(f"✅ Copied '{filename}' from '{source_dir}' to '{destination_dir}'")

        except Exception as e:
            logging.error(f"❌ Error copying '{filename}': {e}")
            all_files_found = False

    if not all_files_found:
        logging.error("❌ Not all required raw data files were found or copied successfully. Exiting.")
        sys.exit(1) # Exit with an error code if not all files are present
    else:
        logging.info("🎉 All required raw data files successfully copied to the input staging area.")

if __name__ == "__main__":
    # IMPORTANT:
    # Ensure your initial raw CSV files are located in the 'data' directory
    # relative to where you run this script.
    # Example directory structure:
    # project_root/
    # ├── chain_setup_inputs.py
    # └── data/
    #     ├── batters_home.csv
    #     ├── batters_away.csv
    #     ├── pitchers_home.csv
    #     ├── pitchers_away.csv
    #     └── games.csv
    #
    # This script will copy them into a new 'raw_data' directory.

    setup_raw_data_inputs(source_dir='data', destination_dir='raw_data')

