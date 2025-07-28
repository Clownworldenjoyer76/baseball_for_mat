# bat_1.py
import pandas as pd
import os

def process_bat_file(input_filepath, output_filepath):
    """
    Deletes columns ending in '_y' and renames columns ending in '_x'
    for a given batting data file.

    Args:
        input_filepath (str): Path to the input CSV file.
        output_filepath (str): Path to the output CSV file.
    """
    print(f"🔄 Processing {input_filepath}...")
    try:
        df = pd.read_csv(input_filepath)
    except FileNotFoundError:
        print(f"❌ Error: Input file not found at {input_filepath}")
        return
    except Exception as e:
        print(f"❌ Error reading {input_filepath}: {e}")
        return

    # Task 1: Delete all columns ending in _y
    columns_to_drop = [col for col in df.columns if col.endswith('_y')]
    if columns_to_drop:
        df.drop(columns=columns_to_drop, inplace=True)
        print(f"  🗑️ Dropped columns ending in '_y': {columns_to_drop}")
    else:
        print("  ✅ No columns ending in '_y' found to drop.")

    # Task 2: Rename column headers ending in _x to remove the _x suffix
    rename_mapping = {}
    for col in df.columns:
        if col.endswith('_x'):
            new_col_name = col[:-2]  # Remove the last two characters (_x)
            rename_mapping[col] = new_col_name
    
    if rename_mapping:
        df.rename(columns=rename_mapping, inplace=True)
        print(f"  ✏️ Renamed columns ending in '_x': {rename_mapping}")
    else:
        print("  ✅ No columns ending in '_x' found to rename.")

    # Ensure output directory exists
    output_dir = os.path.dirname(output_filepath)
    os.makedirs(output_dir, exist_ok=True)

    # Save the updated DataFrame to the output file
    try:
        df.to_csv(output_filepath, index=False)
        print(f"✅ Successfully saved processed data to {output_filepath}")
    except Exception as e:
        print(f"❌ Error saving to {output_filepath}: {e}")


if __name__ == "__main__":
    # Define input and output file paths
    input_batawp = 'data/end_chain/final/finalbatawp.csv'
    output_bat_away1 = 'data/end_chain/final/updating/bat_away1.csv'

    input_bathwp = 'data/end_chain/final/finalbathwp.csv'
    output_bat_home1 = 'data/end_chain/final/updating/bat_home1.csv'

    # Process finalbatawp.csv
    process_bat_file(input_batawp, output_bat_away1)

    print("-" * 30) # Separator for clarity

    # Process finalbathwp.csv
    process_bat_file(input_bathwp, output_bat_home1)

    print("\nProcessing complete for bat_1.py.")

