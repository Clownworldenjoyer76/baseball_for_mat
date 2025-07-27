import pandas as pd
import os

def update_batters_away(
    batters_today_path: str, todaysgames_normalized_path: str, batters_away_output_path: str
) -> None:
    """
    Updates the batters_away.csv file with current away team batters from batters_today.csv.
    It preserves the column order of the existing batters_away.csv, but only includes
    columns that are also present in the filtered batters_today.csv data.
    If batters_away.csv does not exist or is empty, its columns will be inferred from batters_today.csv.

    Args:
        batters_today_path (str): Path to the batters_today.csv file.
        todaysgames_normalized_path (str): Path to the todaysgames_normalized.csv file.
        batters_away_output_path (str): Path to the batters_away.csv file to be updated/overwritten.
    """
    
    target_columns_order = None # This will store the desired order of columns from the existing file
    
    # 1. Determine the desired column order for the output file
    # If batters_away.csv already exists and has columns, use them to try and preserve schema/order
    if os.path.exists(batters_away_output_path) and os.path.getsize(batters_away_output_path) > 0:
        try:
            # Read only the header to get existing column names without loading all data
            existing_batters_away_df = pd.read_csv(batters_away_output_path, nrows=0)
            target_columns_order = existing_batters_away_df.columns.tolist()
        except pd.errors.EmptyDataError:
            print(f"Warning: '{batters_away_output_path}' is empty. Columns will be inferred from '{batters_today_path}'.")
        except Exception as e:
            print(f"Error reading columns from '{batters_away_output_path}': {e}. Columns will be inferred from '{batters_today_path}'.")
    else:
        print(f"Info: '{batters_away_output_path}' not found. It will be created, inferring columns from '{batters_today_path}'.")

    # 2. Read input data from batters_today.csv
    try:
        batters_df = pd.read_csv(batters_today_path)
    except FileNotFoundError:
        print(f"Error: Input file not found: '{batters_today_path}'")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: '{batters_today_path}'. No updates performed.")
        return
    except Exception as e:
        print(f"An error occurred reading '{batters_today_path}': {e}")
        return

    # 3. Read input data from todaysgames_normalized.csv
    try:
        games_df = pd.read_csv(todaysgames_normalized_path)
    except FileNotFoundError:
        print(f"Error: Input file not found: '{todaysgames_normalized_path}'")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: '{todaysgames_normalized_path}'. No updates performed.")
        return
    except Exception as e:
        print(f"An error occurred reading '{todaysgames_normalized_path}': {e}")
        return

    # 4. Get unique away team names and filter batters_df
    away_teams = games_df["away_team"].unique()
    filtered_batters_df = batters_df[batters_df["team"].isin(away_teams)].copy()

    # 5. Adjust columns of the filtered data based on the desired output schema
    if target_columns_order is None:
        # If no existing columns were found for batters_away.csv, use all columns from the filtered data
        final_batters_away_df = filtered_batters_df
    else:
        # Select only the columns that exist in both the target order and the filtered data
        # And ensure they are in the order defined by target_columns_order
        columns_to_include = [col for col in target_columns_order if col in filtered_batters_df.columns]
        final_batters_away_df = filtered_batters_df[columns_to_include]
        
        # Note: Columns that were in target_columns_order but not in filtered_batters_df.columns
        # will be implicitly ignored as per the new requirement.

    # 6. Write the updated data to CSV
    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(batters_away_output_path), exist_ok=True)
        
        # Overwrite the batters_away.csv file with the newly filtered and column-adjusted data
        final_batters_away_df.to_csv(batters_away_output_path, index=False)
        print(f"Successfully updated '{batters_away_output_path}' with new away team batters.")
    except Exception as e:
        print(f"Error writing to '{batters_away_output_path}': {e}")

if __name__ == "__main__":
    # Define input and output file paths
    BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
    TODAYS_GAMES_NORMALIZED_FILE = "data/raw/todaysgames_normalized.csv"
    BATTERS_AWAY_OUTPUT_FILE = "data/adjusted/batters_away.csv"

    update_batters_away(BATTERS_TODAY_FILE, TODAYS_GAMES_NORMALIZED_FILE, BATTERS_AWAY_OUTPUT_FILE)
