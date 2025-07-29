import pandas as pd
from pathlib import Path

# === Config ===
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
BAT_TODAY_FILE = Path("data/end_chain/bat_today.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
# Removed BATTERS_TODAY_UPDATE_FILE as it was an incorrect assumption.
# The source for updates is data/cleaned/batters_today.csv as stated by user.
UPDATE_SOURCE_FILE = Path("data/cleaned/batters_today.csv") # Renamed for clarity on its purpose

OUTPUT_DIR = Path("data/end_chain/final/")


# === Input file paths ===
# These are already defined in Config, but keeping them here for consistency with original script structure
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
BAT_TODAY_FILE = Path("data/end_chain/bat_today.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")


# === Output file paths ===
BAT_HOME_FINAL = OUTPUT_DIR / "batter_home_final.csv"
BAT_AWAY_FINAL = OUTPUT_DIR / "batter_away_final.csv"
BAT_TODAY_FINAL = OUTPUT_DIR / "bat_today_final.csv"
PITCHERS_FINAL = OUTPUT_DIR / "startingpitchers_final.csv"

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    return pd.read_csv(path)

def enforce_last_first(name):
    if not isinstance(name, str) or "," not in name:
        parts = name.strip().split()
        if len(parts) >= 2:
            return f"{parts[-1].capitalize()}, {' '.join(p.capitalize() for p in parts[:-1])}"
        return name
    return name.strip()

def main():
    print("🔄 Loading normalized files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    bat_today = load_csv(BAT_TODAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    
    # Load the correct update source file
    batters_today_data = load_csv(UPDATE_SOURCE_FILE) 

    print("✅ Files loaded. Checking formats...")

    # Enforce final name formatting again before final save
    bat_home["pitcher_home"] = bat_home["pitcher_home"].apply(enforce_last_first)
    bat_away["pitcher_away"] = bat_away["pitcher_away"].apply(enforce_last_first)
    bat_today["name"] = bat_today["name"].apply(enforce_last_first)
    bat_home["last_name, first_name"] = bat_home["last_name, first_name"].apply(enforce_last_first)
    bat_away["last_name, first_name"] = bat_away["last_name, first_name"].apply(enforce_last_first)
    bat_today["last_name, first_name"] = bat_today["last_name, first_name"].apply(enforce_last_first)
    pitchers["last_name, first_name"] = pitchers["last_name, first_name"].apply(enforce_last_first)

    print("🔄 Updating batter_home_final.csv and batter_away_final.csv with new values...")

    # Columns to update
    update_columns = [
        "adj_woba_combined", "whiff_percent", "zone_swing_miss_percent",
        "out_of_zone_swing_miss_percent", "gb_percent", "fb_percent",
        "innings_pitched", "strikeouts", "hit", "bb_percent", "double",
        "triple", "home_run"
    ]

    # --- Update bat_home ---
    # Merge to update bat_home using 'player_id' and specific columns from batters_today_data
    # Use update() method for in-place update if column names are identical
    # Otherwise, perform merge and then update
    
    # First, ensure 'player_id' is of consistent type if there are issues
    # For now, assuming they match.
    
    # Select only the relevant columns from the source data for merging
    source_data_for_merge = batters_today_data[["player_id"] + [col for col in update_columns if col in batters_today_data.columns]]

    # Ensure all target columns exist in bat_home before attempting to update them
    # If a column doesn't exist in bat_home, it will be added by the merge, then we can drop it if needed.
    # However, the instruction is "Do not create new columns."
    # So, we should only merge columns that already exist in `bat_home`.
    
    # Identify common columns that need updating
    common_update_cols_bat_home = [col for col in update_columns if col in bat_home.columns and col in source_data_for_merge.columns]

    # Perform the merge operation
    bat_home = pd.merge(
        bat_home,
        source_data_for_merge[['player_id'] + common_update_cols_bat_home],
        on="player_id",
        how="left",
        suffixes=('', '_from_update') # Use a distinct suffix for merged columns
    )

    # Update values for common columns
    for col in common_update_cols_bat_home:
        # Use .update() for in-place update, which handles NaNs properly for existing values.
        # This will replace existing values in bat_home with non-NaN values from the merged column.
        bat_home[col].update(bat_home[col + '_from_update'])
        bat_home.drop(columns=[col + '_from_update'], inplace=True) # Drop the temporary merged column
    
    # --- Update bat_away ---
    common_update_cols_bat_away = [col for col in update_columns if col in bat_away.columns and col in source_data_for_merge.columns]

    bat_away = pd.merge(
        bat_away,
        source_data_for_merge[['player_id'] + common_update_cols_bat_away],
        on="player_id",
        how="left",
        suffixes=('', '_from_update')
    )

    for col in common_update_cols_bat_away:
        bat_away[col].update(bat_away[col + '_from_update'])
        bat_away.drop(columns=[col + '_from_update'], inplace=True)

    print("💾 Saving renamed final files...")
    bat_home.to_csv(BAT_HOME_FINAL, index=False)
    bat_away.to_csv(BAT_AWAY_FINAL, index=False)
    bat_today.to_csv(BAT_TODAY_FINAL, index=False)
    pitchers.to_csv(PITCHERS_FINAL, index=False)

    print("✅ Final files saved to data/end_chain/final/.")

if __name__ == "__main__":
    main()
