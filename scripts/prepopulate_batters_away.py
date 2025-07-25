import pandas as pd
from pathlib import Path

# File paths
BATT_FILE = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
GAMES_FILE = Path("data/end_chain/todaysgames_normalized.csv")
WEATHER_ADJUSTMENTS_FILE = Path("data/weather_adjustments.csv") # Renamed for clarity
WEATHER_INPUT_FILE = Path("data/weather_input.csv") # Added for Park Factor and time_of_day

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    print("🔄 Loading input files...")
    try:
        batters = load_csv(BATT_FILE)
        games = load_csv(GAMES_FILE)
        weather_adjustments = load_csv(WEATHER_ADJUSTMENTS_FILE)
        weather_input = load_csv(WEATHER_INPUT_FILE) # Load weather_input.csv
        print("✅ Files loaded successfully.")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return
    except Exception as e:
        print(f"❌ An unexpected error occurred during file loading: {e}")
        return

    # --- Step 1: Add home_team to batters DataFrame ---
    print("Merging home_team from games data...")
    # Ensure consistency in team names (uppercase and strip whitespace) before merge
    games['away_team'] = games['away_team'].str.strip().str.upper()
    games['home_team'] = games['home_team'].str.strip().str.upper()
    batters['team'] = batters['team'].str.strip().str.upper() # Assuming 'team' in batters is the away team

    # Select necessary columns and drop duplicates in games_subset
    # Note: `todaysgames_normalized.csv` should have one row per game, so drop_duplicates
    # here might not be strictly necessary if it's already clean, but doesn't hurt.
    games_subset = games[["away_team", "home_team"]].drop_duplicates()

    # Merge home_team from games_subset into batters
    # Merge batters (away_team = team) with games (away_team, home_team)
    # This adds the 'home_team' corresponding to the 'away_team' in batters.
    batters = pd.merge(batters, games_subset, left_on="team", right_on="away_team", how="left")

    # Drop the redundant 'away_team' column that came from games_subset
    batters.drop(columns=["away_team"], inplace=True)
    print("✅ home_team merged.")

    # --- Step 2: Merge weather data from weather_adjustments.csv ---
    print("Merging weather adjustment data...")
    # Standardize column names for merging if necessary (e.g., ensure team names are uppercase)
    weather_adjustments['away_team'] = weather_adjustments['away_team'].str.strip().str.upper()
    weather_adjustments['home_team'] = weather_adjustments['home_team'].str.strip().str.upper()

    # Select and rename columns from weather_adjustments for clarity in merge
    # The 'home_team' and 'away_team' columns in weather_adjustments are the *actual* home/away teams for that game's weather data.
    # For batters_away, 'batters['team']' is the away team, and 'batters['home_team']' is the home team for that game.
    weather_adj_subset = weather_adjustments[[
        "away_team",
        "home_team",
        "temperature",
        "humidity",
        "wind_speed",
        "wind_direction",
        "precipitation", # Added as it's a standard weather output
        "condition",     # Added as it's a standard weather output
        "notes",         # Added as it's a standard weather output
        "game_time"      # Added as it's a standard weather output
    ]].drop_duplicates() # Drop duplicates in case weather_adjustments has multiple entries per game

    # Merge weather data from weather_adjustments.csv
    # Merge on the actual home_team and away_team for the game
    batters = pd.merge(
        batters,
        weather_adj_subset,
        left_on=["team", "home_team"], # 'team' in batters is the away_team for this specific script
        right_on=["away_team", "home_team"],
        how="left",
        suffixes=('_batter', '_weather') # Add suffixes to avoid column name clashes if any
    )

    # Drop redundant columns that came from the weather_adj_subset merge if suffixes were applied
    # You might get 'away_team_weather' and 'home_team_weather' if they clash.
    # We want to keep the weather_adj_subset's columns as the primary weather info.
    batters.drop(columns=[col for col in batters.columns if '_weather' in str(col) and col not in ['temperature', 'humidity', 'wind_speed', 'wind_direction', 'precipitation', 'condition', 'notes', 'game_time']], errors='ignore', inplace=True)
    print("✅ Weather adjustment data merged.")


    # --- Step 3: Pull Park Factor and time_of_day from weather_input.csv ---
    print("Merging Park Factor and time_of_day from weather_input data...")
    # weather_input.csv comes from generate_weather_csv and has 'venue', 'location', 'game_time', 'home_team', 'away_team'
    # We need to ensure we join correctly based on the game.
    
    # Standardize team names for merging
    weather_input['away_team'] = weather_input['away_team'].str.strip().str.upper()
    weather_input['home_team'] = weather_input['home_team'].str.strip().str.upper()

    # Select necessary columns from weather_input
    # 'home_team' and 'away_team' define the specific game.
    # 'Park Factor' and 'time_of_day' are pulled directly.
    weather_input_subset = weather_input[[
        "home_team",
        "away_team",
        "Park Factor",
        "game_time", # Ensure game_time is present for a unique match
        "notes"      # 'notes' indicates roof open/closed, which is related to park factor
    ]].drop_duplicates() # Drop duplicates in case weather_input has multiple entries per game

    # Merge Park Factor and time_of_day
    # Merging on away_team and home_team to identify the specific game
    batters = pd.merge(
        batters,
        weather_input_subset,
        left_on=["team", "home_team"], # 'team' in batters is the away_team, 'home_team' is the game's home team
        right_on=["away_team", "home_team"],
        how="left",
        suffixes=('_current', '_input') # Suffixes for this merge
    )

    # Clean up duplicate columns from the merge if suffixes created them and they're not needed.
    # We now have 'game_time_current' (from weather_adjustments) and 'game_time_input'.
    # Decide which one to keep if both are present. Typically, 'game_time' from weather_adjustments
    # would be the actual one pulled, while weather_input's game_time is the reference.
    # I'll prioritize game_time from weather_adjustments if it exists, otherwise use _input.
    if 'game_time_current' in batters.columns and 'game_time_input' in batters.columns:
        batters['game_time'] = batters['game_time_current'].fillna(batters['game_time_input'])
        batters.drop(columns=['game_time_current', 'game_time_input'], inplace=True)
    elif 'game_time_input' in batters.columns:
        batters.rename(columns={'game_time_input': 'game_time'}, inplace=True)

    # For 'notes', if 'notes_current' (from weather_adjustments) and 'notes_input' (from weather_input) exist,
    # combine or prioritize. It's likely you want the 'notes' from weather_adjustments if that's current weather.
    # If notes from weather_input (roof open/closed) is what you want for Park Factor context, use that.
    # Let's assume you want the 'notes' from weather_adjustments, but keep 'Park Factor' from weather_input.
    batters.drop(columns=[col for col in batters.columns if '_input' in str(col) and col not in ['Park Factor', 'time_of_day']], errors='ignore', inplace=True)
    batters.drop(columns=['away_team_input'], errors='ignore', inplace=True) # Drop redundant team columns from merge
    print("✅ Park Factor and time_of_day merged.")

    # Final check for expected columns
    expected_cols = ["team", "home_team", "temperature", "humidity", "wind_speed", "wind_direction",
                     "precipitation", "condition", "notes", "game_time", "Park Factor", "time_of_day"]
    missing_cols = [col for col in expected_cols if col not in batters.columns]
    if missing_cols:
        print(f"⚠️ Warning: Some expected columns are missing after merges: {', '.join(missing_cols)}")


    # Save updated DataFrame
    batters.to_csv(BATT_FILE, index=False)
    print(f"✅ Updated {BATT_FILE.name}")

if __name__ == "__main__":
    main()
