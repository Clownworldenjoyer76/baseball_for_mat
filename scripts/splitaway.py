import pandas as pd
import os

def populate_away_batters(
    batters_today_path: str, todaysgames_normalized_path: str, batters_away_output_path: str
) -> None:
    """
    Populates batters_away.csv with rows from batters_today.csv where the 'team'
    matches an 'away_team' from todaysgames_normalized.csv.
    All columns from the matching rows in batters_today.csv will be included.
    Includes robust string cleaning and detailed logging for debugging.

    Args:
        batters_today_path (str): Path to the batters_today.csv file.
        todaysgames_normalized_path (str): Path to the todaysgames_normalized.csv file.
        batters_away_output_path (str): Path where the resulting batters_away.csv will be saved.
    """
    print(f"--- Starting populate_away_batters for {batters_away_output_path} ---")

    # --- Load batters_today.csv ---
    try:
        batters_df = pd.read_csv(batters_today_path)
        print(f"Read '{batters_today_path}'. Initial shape: {batters_df.shape}")
        if batters_df.empty:
            print(f"Error: '{batters_today_path}' is empty. No batters to process.")
            return
        if 'team' not in batters_df.columns:
            print(f"Error: '{batters_today_path}' does not contain a 'team' column. Columns found: {batters_df.columns.tolist()}")
            return
        # Clean 'team' column for consistent matching
        batters_df['team_cleaned'] = batters_df['team'].astype(str).str.strip().str.upper()
        print(f"Unique teams in {batters_today_path} (cleaned): {batters_df['team_cleaned'].unique().tolist()}")

    except FileNotFoundError:
        print(f"Error: Input file not found: '{batters_today_path}'. Please ensure the path is correct and file exists.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: '{batters_today_path}'. No batters to process.")
        return
    except Exception as e:
        print(f"An unexpected error occurred reading '{batters_today_path}': {e}")
        return

    # --- Load todaysgames_normalized.csv ---
    try:
        games_df = pd.read_csv(todaysgames_normalized_path)
        print(f"Read '{todaysgames_normalized_path}'. Initial shape: {games_df.shape}")
        if games_df.empty:
            print(f"Error: '{todaysgames_normalized_path}' is empty. Cannot determine away teams.")
            return
        if 'away_team' not in games_df.columns:
            print(f"Error: '{todaysgames_normalized_path}' does not contain an 'away_team' column. Columns found: {games_df.columns.tolist()}")
            return
        # Clean 'away_team' column for consistent matching
        games_df['away_team_cleaned'] = games_df['away_team'].astype(str).str.strip().str.upper()
        print(f"Unique away teams in {todaysgames_normalized_path} (cleaned): {games_df['away_team_cleaned'].unique().tolist()}")

    except FileNotFoundError:
        print(f"Error: Input file not found: '{todaysgames_normalized_path}'. Please ensure the path is correct and file exists.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: '{todaysgames_normalized_path}'. Cannot determine away teams.")
        return
    except Exception as e:
        print(f"An unexpected error occurred reading '{todaysgames_normalized_path}': {e}")
        return

    # --- Get away teams and filter batters ---
    away_teams_cleaned = games_df["away_team_cleaned"].unique()
    print(f"Identified {len(away_teams_cleaned)} unique CLEANED away teams: {away_teams_cleaned.tolist()}")
    
    # Filter batters_df using the cleaned 'team' column
    away_batters_df = batters_df[batters_df["team_cleaned"].isin(away_teams_cleaned)].copy()
    
    # Drop the temporary cleaned team column before saving
    away_batters_df = away_batters_df.drop(columns=['team_cleaned'])

    print(f"Filtered batters for away teams. Resulting DataFrame shape: {away_batters_df.shape}")

    if away_batters_df.empty:
        print(f"WARNING: No away team batters found after filtering. The output file will contain only headers.")
        # To ensure an empty file with correct headers is written if no data matches
        if not batters_df.empty:
            away_batters_df = pd.DataFrame(columns=batters_df.drop(columns=['team_cleaned']).columns)
        else: # If batters_df was already empty, just pass an empty df (no columns)
            away_batters_df = pd.DataFrame()


    # --- Ensure output directory exists ---
    output_dir = os.path.dirname(batters_away_output_path)
    if output_dir: # Check if output_dir is not empty (i.e., path is not just a filename)
        os.makedirs(output_dir, exist_ok=True)
        print(f"Ensured directory '{output_dir}' exists.")

    # --- Write the updated data to CSV ---
    try:
        away_batters_df.to_csv(batters_away_output_path, index=False)
        print(f"SUCCESS: Populated '{batters_away_output_path}' with {away_batters_df.shape[0]} away team batters.")
    except Exception as e:
        print(f"ERROR: Failed to write to '{batters_away_output_path}': {e}")

    print(f"--- Finished populate_away_batters ---")


if __name__ == "__main__":
    # Define input and output file paths
    BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
    TODAYS_GAMES_NORMALIZED_FILE = "data/raw/todaysgames_normalized.csv"
    BATTERS_AWAY_OUTPUT_FILE = "data/adjusted/batters_away.csv"

    populate_away_batters(BATTERS_TODAY_FILE, TODAYS_GAMES_NORMALIZED_FILE, BATTERS_AWAY_OUTPUT_FILE)
