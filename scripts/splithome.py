import pandas as pd
import os
import sys

def populate_home_batters(
    batters_today_path: str, todaysgames_normalized_path: str, batters_home_output_path: str
) -> None:
    """
    Populates batters_home.csv with rows from batters_today.csv where the 'team'
    matches a 'home_team' from todaysgames_normalized.csv.
    All columns from the matching rows in batters_today.csv will be included.
    Includes robust string cleaning and detailed logging for debugging.

    Args:
        batters_today_path (str): Path to the batters_today.csv file.
        todaysgames_normalized_path (str): Path to the todaysgames_normalized.csv file.
        batters_home_output_path (str): Path where the resulting batters_home.csv will be saved.
    """
    print(f"--- Starting populate_home_batters for {batters_home_output_path} ---")
    print(f"Using batters_today from: {batters_today_path}")
    print(f"Using todaysgames_normalized from: {todaysgames_normalized_path}")

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
            print(f"Error: '{todaysgames_normalized_path}' is empty. Cannot determine home teams.")
            return
        if 'home_team' not in games_df.columns: # <--- Changed from 'away_team' to 'home_team'
            print(f"Error: '{todaysgames_normalized_path}' does not contain a 'home_team' column. Columns found: {games_df.columns.tolist()}")
            return
        # Clean 'home_team' column for consistent matching
        games_df['home_team_cleaned'] = games_df['home_team'].astype(str).str.strip().str.upper() # <--- Changed from 'away_team' to 'home_team'
        print(f"Unique home teams in {todaysgames_normalized_path} (cleaned): {games_df['home_team_cleaned'].unique().tolist()}") # <--- Changed from 'away_team' to 'home_team'

    except FileNotFoundError:
        print(f"Error: Input file not found: '{todaysgames_normalized_path}'. Please ensure the path is correct and file exists.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: '{todaysgames_normalized_path}'. Cannot determine home teams.")
        return
    except Exception as e:
        print(f"An unexpected error occurred reading '{todaysgames_normalized_path}': {e}")
        return

    # --- Get home teams and filter batters ---
    home_teams_cleaned = games_df["home_team_cleaned"].unique() # <--- Changed from 'away_teams' to 'home_teams'
    print(f"Identified {len(home_teams_cleaned)} unique CLEANED home teams: {home_teams_cleaned.tolist()}") # <--- Changed from 'away_teams' to 'home_teams'
    
    # Filter batters_df using the cleaned 'team' column
    home_batters_df = batters_df[batters_df["team_cleaned"].isin(home_teams_cleaned)].copy() # <--- Changed from 'away_batters_df' to 'home_batters_df'
    
    # Drop the temporary cleaned team column before saving
    home_batters_df = home_batters_df.drop(columns=['team_cleaned']) # <--- Changed from 'away_batters_df' to 'home_batters_df'

    print(f"Filtered batters for home teams. Resulting DataFrame shape: {home_batters_df.shape}") # <--- Changed from 'away_batters_df' to 'home_batters_df'

    if home_batters_df.empty: # <--- Changed from 'away_batters_df' to 'home_batters_df'
        print(f"WARNING: No home team batters found after filtering. The output file will contain only headers.")
        # To ensure an empty file with correct headers is written if no data matches
        if not batters_df.empty:
            home_batters_df = pd.DataFrame(columns=batters_df.drop(columns=['team_cleaned']).columns)
        else:
            home_batters_df = pd.DataFrame()


    # --- Ensure output directory exists ---
    output_dir = os.path.dirname(batters_home_output_path) # <--- Changed output path variable
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Ensured directory '{output_dir}' exists.")

    # --- Write the updated data to CSV ---
    try:
        home_batters_df.to_csv(batters_home_output_path, index=False) # <--- Changed df and output path
        print(f"SUCCESS: Populated '{batters_home_output_path}' with {home_batters_df.shape[0]} home team batters.") # <--- Changed df and output path
    except Exception as e:
        print(f"ERROR: Failed to write to '{batters_home_output_path}': {e}") # <--- Changed output path

    print(f"--- Finished populate_home_batters ---")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python splithome.py <batters_today_path> <todaysgames_normalized_path> <batters_home_output_path>")
        # Fallback to hardcoded paths if run directly without args, but prefer args for GH Actions
        BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
        TODAYS_GAMES_NORMALIZED_FILE = "data/raw/todaysgames_normalized.csv"
        BATTERS_HOME_OUTPUT_FILE = "data/adjusted/batters_home.csv" # <--- Changed output file
        print("Falling back to default paths for local execution.")
        populate_home_batters(BATTERS_TODAY_FILE, TODAYS_GAMES_NORMALIZED_FILE, BATTERS_HOME_OUTPUT_FILE)
    else:
        populate_home_batters(sys.argv[1], sys.argv[2], sys.argv[3])

