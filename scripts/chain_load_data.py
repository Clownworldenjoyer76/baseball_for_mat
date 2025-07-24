# chain_load_data.py

import pandas as pd
import os
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_and_save_raw_data(data_dir='raw_data'):
    """
    Loads raw baseball data from CSV files and saves them to a specified directory.

    Args:
        data_dir (str): The directory where raw data files will be saved.
                        This directory will be created if it doesn't exist.
    """
    logging.info("Starting chain_load_data.py script.")

    # Define the input CSV file paths.
    # IMPORTANT: Adjust these paths to where your actual raw CSV files are located.
    # For demonstration, assuming they are in a 'data' subfolder relative to the script.
    input_csv_paths = {
        'batters_home': 'data/batters_home.csv',
        'batters_away': 'data/batters_away.csv',
        'pitchers_home': 'data/pitchers_home.csv',
        'pitchers_away': 'data/pitchers_away.csv',
        'games': 'data/games.csv',
    }

    # Create the output directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    logging.info(f"Ensured output directory '{data_dir}' exists.")

    loaded_dataframes = {}
    for name, path in input_csv_paths.items():
        try:
            # Check if the file exists before attempting to read
            if not os.path.exists(path):
                raise FileNotFoundError(f"File not found: {path}")

            df = pd.read_csv(path)
            loaded_dataframes[name] = df
            logging.info(f"✅ Loaded {name}: {len(df)} rows")

            # Save the loaded DataFrame to the raw_data directory
            output_path = os.path.join(data_dir, f'{name}.csv')
            df.to_csv(output_path, index=False)
            logging.info(f"Saved raw {name} to {output_path}")

        except FileNotFoundError as e:
            logging.error(f"❌ Error loading {name}: {e}")
            sys.exit(1) # Exit if a critical file is missing
        except pd.errors.EmptyDataError:
            logging.error(f"❌ Error loading {name}: CSV file is empty at {path}")
            sys.exit(1)
        except pd.errors.ParserError as e:
            logging.error(f"❌ Error parsing {name} from {path}: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ An unexpected error occurred while loading {name} from {path}: {e}")
            sys.exit(1)

    logging.info("🎉 All raw data loaded and saved successfully.")
    return loaded_dataframes # Optional: return dataframes if this script is imported

if __name__ == "__main__":
    # Example usage:
    # Ensure you have a 'data' directory in the same location as this script,
    # and your raw CSV files inside it.
    # Example structure:
    # project_root/
    # ├── chain_load_data.py
    # └── data/
    #     ├── batters_home.csv
    #     ├── batters_away.csv
    #     ├── pitchers_home.csv
    #     ├── pitchers_away.csv
    #     └── games.csv

    load_and_save_raw_data()
