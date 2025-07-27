import pandas as pd
import os

def update_batters_away(
    batters_today_path: str, todaysgames_normalized_path: str, batters_away_output_path: str
) -> None:
    """
    Updates the batters_away.csv file with current away team batters from batters_today.csv,
    while preserving the original column structure of batters_away.csv.

    Args:
        batters_today_path (str): Path to the batters_today.csv file.
        todaysgames_normalized_path (str): Path to the todaysgames_normalized.csv file.
        batters_away_output_path (str): Path to the batters_away.csv file to be updated/overwritten.
    """
    original_away_columns = None
    try:
        # Read the existing batters_away.csv to get its column names
        # Use a dummy read if file might not exist to just get header, or handle gracefully
        if os.path.exists(batters_away_output_path) and os.path.getsize(batters_away_output_path) > 0:
            original_away_df = pd.read_csv(batters_away_output_path, nrows=0) # Read only header
            original_away_columns = original_away_df.columns.tolist()
        else:
            print(f"Warning: {batters_away_output_path} not found or is empty. Columns will be determined by {batters_today_path}.")
            # If the file doesn't exist, we will use columns from batters_today.csv as a fallback
            # but ideally, this file should exist with a predefined schema.
    except pd.errors.EmptyDataError:
        print(f"Warning: {batters_away_output_path} is empty. Columns will be determined by {batters_today_path}.")
    except Exception as e:
        print(f"An error occurred reading columns from {batters_away_output_path}: {e}")
        print("Proceeding, but column preservation might be affected.")


    try:
        # Read batters_today.csv to get the source data for away batters
        batters_df = pd.read_csv(batters_today_path)
    except FileNotFoundError:
        print(f"Error: Input file not found: {batters_today_path}")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: {batters_today_path}. No updates performed.")
        return
    except Exception as e:
        print(f"An error occurred reading {batters_today_path}: {e}")
        return

    try:
        # Read todaysgames_normalized.csv to get away team names
        games_df = pd.read_csv(todaysgames_normalized_path)
    except FileNotFoundError:
        print(f"Error: Input file not found: {todaysgames_normalized_path}")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: {todaysgames_normalized_path}. No updates performed.")
        return
    except Exception as e:
        print(f"An error occurred reading {todaysgames_normalized_path}: {e}")
        return

    # Get unique away team names from todaysgames_normalized.csv
    away_teams = games_df["away_team"].unique()

    # Filter batters_df to include only players whose 'team' matches an 'away_team'
    filtered_batters_df = batters_df[batters_df["team"].isin(away_teams)].copy()

    # Preserve original columns if they were successfully read
    if original_away_columns is not None:
        # Select only the columns that exist in the original batters_away.csv
        # and are also present in the filtered_batters_df
        cols_to_keep = [col for col in original_away_columns if col in filtered_batters_df.columns]
        
        # If any columns from original_away_columns are missing in filtered_batters_df,
        # they will be added as NaN columns to maintain schema.
        missing_cols = [col for col in original_away_columns if col not in filtered_batters_df.columns]
        for col in missing_cols:
            filtered_batters_df[col] = pd.NA # Or None, or a default value, depending on desired behavior

        # Reorder columns to match the original batters_away.csv
        final_batters_away_df = filtered_batters_df[original_away_columns]
    else:
        # Fallback: if original_away_columns couldn't be determined, use all columns from filtered data
        # This deviates from "do not update the columns" if the file was truly missing/empty.
        final_batters_away_df = filtered_batters_df
        print(f"Warning: Could not determine original columns for {batters_away_output_path}. Using all columns from {batters_today_path}.")


    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(batters_away_output_path), exist_ok=True)
        
        # Overwrite the batters_away.csv file with the newly filtered and column-adjusted data
        final_batters_away_df.to_csv(batters_away_output_path, index=False)
        print(f"Successfully updated {batters_away_output_path} with new away team batters, preserving column structure.")
    except Exception as e:
        print(f"Error writing to {batters_away_output_path}: {e}")

if __name__ == "__main__":
    # Define input and output file paths
    BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
    TODAYS_GAMES_NORMALIZED_FILE = "data/raw/todaysgames_normalized.csv"
    BATTERS_AWAY_OUTPUT_FILE = "data/adjusted/batters_away.csv"

    update_batters_away(BATTERS_TODAY_FILE, TODAYS_GAMES_NORMALIZED_FILE, BATTERS_AWAY_OUTPUT_FILE)
