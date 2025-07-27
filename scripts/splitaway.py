import pandas as pd
import os

def update_batters_away(
    batters_today_path: str, todaysgames_normalized_path: str, batters_away_output_path: str
) -> None:
    """
    Updates the batters_away.csv file by identifying away team batters
    from batters_today.csv based on todaysgames_normalized.csv.

    Args:
        batters_today_path (str): Path to the batters_today.csv file.
        todaysgames_normalized_path (str): Path to the todaysgames_normalized.csv file.
        batters_away_output_path (str): Path to the batters_away.csv file to be updated/overwritten.
    """
    try:
        # Read batters_today.csv
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
        # Read todaysgames_normalized.csv to get away teams
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
    # This creates the new content for batters_away.csv
    new_batters_away_df = batters_df[batters_df["team"].isin(away_teams)].copy()

    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(batters_away_output_path), exist_ok=True)
        
        # Overwrite the batters_away.csv file with the newly filtered data
        new_batters_away_df.to_csv(batters_away_output_path, index=False)
        print(f"Successfully updated {batters_away_output_path} with new away team batters.")
    except Exception as e:
        print(f"Error writing to {batters_away_output_path}: {e}")

if __name__ == "__main__":
    # Define input and output file paths
    BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
    TODAYS_GAMES_NORMALIZED_FILE = "data/raw/todaysgames_normalized.csv"
    BATTERS_AWAY_OUTPUT_FILE = "data/adjusted/batters_away.csv"

    update_batters_away(BATTERS_TODAY_FILE, TODAYS_GAMES_NORMALIZED_FILE, BATTERS_AWAY_OUTPUT_FILE)
