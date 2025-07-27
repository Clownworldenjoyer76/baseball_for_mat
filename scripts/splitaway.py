import pandas as pd
import os

def populate_away_batters(
    batters_today_path: str, todaysgames_normalized_path: str, batters_away_output_path: str
) -> None:
    """
    Populates batters_away.csv with rows from batters_today.csv where the 'team'
    matches an 'away_team' from todaysgames_normalized.csv.
    All columns from the matching rows in batters_today.csv will be included.

    Args:
        batters_today_path (str): Path to the batters_today.csv file.
        todaysgames_normalized_path (str): Path to the todaysgames_normalized.csv file.
        batters_away_output_path (str): Path where the resulting batters_away.csv will be saved.
    """
    try:
        # Read batters_today.csv - This is our source for the players' data
        batters_df = pd.read_csv(batters_today_path)
    except FileNotFoundError:
        print(f"Error: Input file not found: '{batters_today_path}'. Please ensure it exists.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: '{batters_today_path}'. No batters to process.")
        return
    except Exception as e:
        print(f"An unexpected error occurred reading '{batters_today_path}': {e}")
        return

    try:
        # Read todaysgames_normalized.csv - This tells us which teams are away teams
        games_df = pd.read_csv(todaysgames_normalized_path)
    except FileNotFoundError:
        print(f"Error: Input file not found: '{todaysgames_normalized_path}'. Please ensure it exists.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: '{todaysgames_normalized_path}'. Cannot determine away teams.")
        return
    except Exception as e:
        print(f"An unexpected error occurred reading '{todaysgames_normalized_path}': {e}")
        return

    # Get the unique list of away team names from the games data
    away_teams = games_df["away_team"].unique()
    
    # Filter batters_df: keep only rows where the 'team' column is in our list of away_teams
    away_batters_df = batters_df[batters_df["team"].isin(away_teams)].copy()

    # Ensure the output directory exists before saving the file
    output_dir = os.path.dirname(batters_away_output_path)
    if output_dir: # Check if output_dir is not empty (i.e., path is not just a filename)
        os.makedirs(output_dir, exist_ok=True)

    try:
        # Save the filtered DataFrame to batters_away.csv, overwriting if it exists
        away_batters_df.to_csv(batters_away_output_path, index=False)
        print(f"Successfully populated '{batters_away_output_path}' with away team batters from '{batters_today_path}'.")
    except Exception as e:
        print(f"Error writing to '{batters_away_output_path}': {e}")

if __name__ == "__main__":
    # Define input and output file paths
    BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
    TODAYS_GAMES_NORMALIZED_FILE = "data/raw/todaysgames_normalized.csv"
    BATTERS_AWAY_OUTPUT_FILE = "data/adjusted/batters_away.csv"

    populate_away_batters(BATTERS_TODAY_FILE, TODAYS_GAMES_NORMALIZED_FILE, BATTERS_AWAY_OUTPUT_FILE)
