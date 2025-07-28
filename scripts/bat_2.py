# bat_2.py
import pandas as pd
import os

def process_bat_home_file(input_filepath, output_filepath):
    """
    Processes bat_home1.csv:
    1. Conditionally drops empty base columns and renames corresponding _input/_adj columns.
    2. Adds new empty columns.
    """
    print(f"🔄 Processing {input_filepath} for bat_home2.csv...")
    try:
        df = pd.read_csv(input_filepath)
    except FileNotFoundError:
        print(f"❌ Error: Input file not found at {input_filepath}")
        return
    except Exception as e:
        print(f"❌ Error reading {input_filepath}: {e}")
        return

    columns_to_drop_later = []
    rename_map = {}
    
    # Task 1: Conditional Drop and Rename for _input and _adj columns
    print("  Checking for _input and _adj columns for conditional processing...")
    current_columns = df.columns.tolist() 
    
    # Process _input columns first
    for col in current_columns:
        if col.endswith('_input'):
            base_col = col[:-len('_input')]
            if base_col in df.columns:
                if df[base_col].isnull().all():
                    columns_to_drop_later.append(base_col)
                    rename_map[col] = base_col
                    print(f"    - Base column '{base_col}' is empty. Will drop '{base_col}' and rename '{col}' to '{base_col}'.")
                else:
                    print(f"    - Base column '{base_col}' is NOT empty. Keeping both '{base_col}' and '{col}'.")
            else:
                print(f"    - No base column '{base_col}' found for '{col}'. Skipping conditional rename.")

    # Process _adj columns
    for col in current_columns:
        if col.endswith('_adj'):
            base_col = col[:-len('_adj')]
            if base_col in df.columns and base_col not in rename_map.values():
                if df[base_col].isnull().all():
                    columns_to_drop_later.append(base_col)
                    rename_map[col] = base_col
                    print(f"    - Base column '{base_col}' is empty. Will drop '{base_col}' and rename '{col}' to '{base_col}'.")
                else:
                    print(f"    - Base column '{base_col}' is NOT empty. Keeping both '{base_col}' and '{col}'.")
            elif base_col not in df.columns:
                print(f"    - No base column '{base_col}' found for '{col}'. Skipping conditional rename.")

    # Apply all collected drops and renames
    if columns_to_drop_later:
        df.drop(columns=columns_to_drop_later, errors='ignore', inplace=True)
        print(f"  🗑️ Dropped base columns: {columns_to_drop_later}")
    if rename_map:
        df.rename(columns=rename_map, inplace=True)
        print(f"  ✏️ Renamed columns: {rename_map}")
    if not columns_to_drop_later and not rename_map:
        print("  ✅ No conditional drops or renames applied.")


    # Task 2: Add Columns headers (Removed 'stadium')
    new_home_columns = ['location', 'notes', 'precipitation']
    for col in new_home_columns:
        if col not in df.columns:
            df[col] = None # Add as empty column
            print(f"  ➕ Added column: '{col}'")
        else:
            print(f"  ⚠️ Column '{col}' already exists. Skipping addition.")


    # Task 3: Delete Columns (removed away_team_games)
    # The section to delete 'away_team_games' has been removed.
    print("  ✅ Skipping deletion of 'away_team_games' column as requested.")


    # Ensure output directory exists
    output_dir = os.path.dirname(output_filepath)
    os.makedirs(output_dir, exist_ok=True)

    # Save the updated DataFrame to the output file
    try:
        df.to_csv(output_filepath, index=False)
        print(f"✅ Successfully saved processed data to {output_filepath}")
    except Exception as e:
        print(f"❌ Error saving to {output_filepath}: {e}")

def process_bat_away_file(input_filepath, output_filepath):
    """
    Processes bat_away1.csv:
    1. Adds new empty columns (excluding 'pitcher_home' and 'pitcher_away').
    2. Removes the 'stadium' column.
    3. Renames pitcher_away_games and pitcher_home_games columns.
    """
    print(f"🔄 Processing {input_filepath} for bat_away2.csv...")
    try:
        df = pd.read_csv(input_filepath)
    except FileNotFoundError:
        print(f"❌ Error: Input file not found at {input_filepath}")
        return
    except Exception as e:
        print(f"❌ Error reading {input_filepath}: {e}")
        return

    # Add Columns headers
    # Removed 'pitcher_home' and 'pitcher_away' from the list of new columns to add
    new_away_columns = [
        'latitude', 'longitude', 'city', 'state', 'timezone', 'is_dome'
    ]
    for col in new_away_columns:
        if col not in df.columns:
            df[col] = None # Add as empty column
            print(f"  ➕ Added column: '{col}'")
        else:
            print(f"  ⚠️ Column '{col}' already exists. Skipping addition.")

    # Remove 'stadium' column
    columns_to_delete_away = ['stadium']
    for col in columns_to_delete_away:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
            print(f"  🗑️ Deleted column: '{col}'")
        else:
            print(f"  ✅ Column '{col}' not found for deletion.")

    # NEW: Update headers to remove '_games' suffix
    rename_games_columns = {}
    if 'pitcher_away_games' in df.columns:
        rename_games_columns['pitcher_away_games'] = 'pitcher_away'
    if 'pitcher_home_games' in df.columns:
        rename_games_columns['pitcher_home_games'] = 'pitcher_home'

    if rename_games_columns:
        df.rename(columns=rename_games_columns, inplace=True)
        print(f"  ✏️ Renamed columns (removed '_games' suffix): {rename_games_columns}")
    else:
        print("  ✅ No '_games' suffix columns found to rename.")

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
    # For bat_home1.csv
    input_bat_home1 = 'data/end_chain/final/updating/bat_home1.csv'
    output_bat_home2 = 'data/end_chain/final/updating/bat_home2.csv'

    # For bat_away1.csv
    input_bat_away1 = 'data/end_chain/final/updating/bat_away1.csv'
    output_bat_away2 = 'data/end_chain/final/updating/bat_away2.csv'

    # Process bat_home1.csv to bat_home2.csv
    process_bat_home_file(input_bat_home1, output_bat_home2)

    print("\n" + "=" * 50 + "\n") # Big separator for clarity

    # Process bat_away1.csv to bat_away2.csv
    process_bat_away_file(input_bat_away1, output_bat_away2)

    print("\nProcessing complete for bat_2.py.")
