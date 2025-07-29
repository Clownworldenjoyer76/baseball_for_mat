import pandas as pd
import os

def normalize_home_team_case(df):
    if "home_team" in df.columns:
        df["home_team"] = df["home_team"].astype(str).str.title()
        print("✅ Normalized 'home_team' to title case.")
    else:
        print("⚠️ 'home_team' column not found in DataFrame.")
    return df

def process_bat_home_file(input_filepath, output_filepath):
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
    
    # Task 1: Conditional Drop and Rename
    print("  Checking for _input and _adj columns for conditional processing...")
    current_columns = df.columns.tolist() 
    for col in current_columns:
        if col.endswith('_input'):
            base_col = col[:-7]
            if base_col in df.columns and df[base_col].isnull().all():
                columns_to_drop_later.append(base_col)
                rename_map[col] = base_col
                print(f"    - Dropping '{base_col}' and renaming '{col}' to '{base_col}'")
        elif col.endswith('_adj'):
            base_col = col[:-4]
            if base_col in df.columns and df[base_col].isnull().all():
                columns_to_drop_later.append(base_col)
                rename_map[col] = base_col
                print(f"    - Dropping '{base_col}' and renaming '{col}' to '{base_col}'")

    if columns_to_drop_later:
        df.drop(columns=columns_to_drop_later, errors='ignore', inplace=True)
        print(f"  🗑️ Dropped: {columns_to_drop_later}")
    if rename_map:
        df.rename(columns=rename_map, inplace=True)
        print(f"  ✏️ Renamed: {rename_map}")
    if not columns_to_drop_later and not rename_map:
        print("  ✅ No conditional drops or renames.")

    # Task 2: Add new empty columns
    new_home_columns = ['location', 'notes', 'precipitation']
    for col in new_home_columns:
        if col not in df.columns:
            df[col] = None
            print(f"  ➕ Added column: '{col}'")

    # Task 3: Normalize home_team casing
    df = normalize_home_team_case(df)

    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    try:
        df.to_csv(output_filepath, index=False)
        print(f"✅ Saved processed data to {output_filepath}")
    except Exception as e:
        print(f"❌ Error saving: {e}")

def process_bat_away_file(input_filepath, output_filepath):
    print(f"🔄 Processing {input_filepath} for bat_away2.csv...")
    try:
        df = pd.read_csv(input_filepath)
    except FileNotFoundError:
        print(f"❌ Error: Input file not found at {input_filepath}")
        return
    except Exception as e:
        print(f"❌ Error reading {input_filepath}: {e}")
        return

    new_away_columns = ['latitude', 'longitude', 'city', 'state', 'timezone', 'is_dome']
    for col in new_away_columns:
        if col not in df.columns:
            df[col] = None
            print(f"  ➕ Added column: '{col}'")

    if 'stadium' in df.columns:
        df.drop(columns=['stadium'], inplace=True)
        print("  🗑️ Deleted column: 'stadium'")

    rename_games_columns = {}
    if 'pitcher_away_games' in df.columns:
        rename_games_columns['pitcher_away_games'] = 'pitcher_away'
    if 'pitcher_home_games' in df.columns:
        rename_games_columns['pitcher_home_games'] = 'pitcher_home'
    if rename_games_columns:
        df.rename(columns=rename_games_columns, inplace=True)
        print(f"  ✏️ Renamed columns: {rename_games_columns}")

    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    try:
        df.to_csv(output_filepath, index=False)
        print(f"✅ Saved processed data to {output_filepath}")
    except Exception as e:
        print(f"❌ Error saving: {e}")

if __name__ == "__main__":
    input_bat_home1 = 'data/end_chain/final/updating/bat_home1.csv'
    output_bat_home2 = 'data/end_chain/final/updating/bat_home2.csv'
    input_bat_away1 = 'data/end_chain/final/updating/bat_away1.csv'
    output_bat_away2 = 'data/end_chain/final/updating/bat_away2.csv'

    process_bat_home_file(input_bat_home1, output_bat_home2)
    print("\n" + "=" * 50 + "\n")
    process_bat_away_file(input_bat_away1, output_bat_away2)
    print("\nProcessing complete for bat_2.py.")
