# bat_1.py
import pandas as pd
import os

def process_bat_file(input_filepath, output_filepath, additional_columns_to_delete=None):
    """
    Deletes columns ending in '_y', renames columns ending in '_x'
    for a given batting data file, and optionally deletes specified additional columns.

    Args:
        input_filepath (str): Path to the input CSV file.
        output_filepath (str): Path to the output CSV file.
        additional_columns_to_delete (list, optional): A list of additional column names to delete.
                                                        Defaults to None.
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

    # Initialize a list for all columns to drop in this specific run
    all_columns_to_drop_this_run = []

    # Task 1: Delete all columns ending in _y
    columns_ending_y = [col for col in df.columns if col.endswith('_y')]
    if columns_ending_y:
        all_columns_to_drop_this_run.extend(columns_ending_y)
        print(f"  🗑️ Identified columns ending in '_y' for deletion: {columns_ending_y}")
    else:
        print("  ✅ No columns ending in '_y' found to drop.")

    # Task 2: Add additional specified columns to delete
    if additional_columns_to_delete:
        for col_to_delete in additional_columns_to_delete:
            if col_to_delete in df.columns:
                all_columns_to_drop_this_run.append(col_to_delete)
        if additional_columns_to_delete: # Only print if some were actually specified
            print(f"  🗑️ Identified additional columns for deletion: {additional_columns_to_delete}")
        else:
            print("  ✅ No additional columns specified for deletion.")

    # Perform the deletion of all identified columns
    if all_columns_to_drop_this_run:
        # Remove duplicates in case a column was identified by multiple rules
        all_columns_to_drop_this_run = list(set(all_columns_to_drop_this_run))
        df.drop(columns=all_columns_to_drop_this_run, inplace=True)
        print(f"  🗑️ Dropped total columns: {all_columns_to_drop_this_run}")
    else:
        print("  ✅ No columns identified for deletion in this step.")


    # Task 3: Rename column headers ending in _x to remove the _x suffix
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
    output_bat_away2 = 'data/end_chain/final/updating/bat_away2.csv' # New output file

    input_bathwp = 'data/end_chain/final/finalbathwp.csv'
    output_bat_home1 = 'data/end_chain/final/updating/bat_home1.csv'

    # Process finalbatawp.csv to bat_away1.csv (original behavior)
    process_bat_file(input_batawp, output_bat_away1)

    print("-" * 30) # Separator for clarity

    # Process finalbatawp.csv to bat_away2.csv with additional columns deleted
    print("Beginning processing for bat_away2.csv with specific column deletions...")
    columns_to_remove_for_bat_away2 = ['pitcher_away', 'pitcher_home', 'team']
    process_bat_file(input_batawp, output_bat_away2, columns_to_remove_for_bat_away2)

    print("-" * 30) # Separator for clarity

    # Process finalbathwp.csv to bat_home1.csv (original behavior)
    process_bat_file(input_bathwp, output_bat_home1)

    print("\nProcessing complete for bat_1.py.")
