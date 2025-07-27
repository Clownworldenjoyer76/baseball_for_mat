# scripts/split_batters_by_home_away.py

import pandas as pd
from pathlib import Path
import re
import logging

# --- Setup ---
BATTERS_FILE = "data/cleaned/batters_today.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MASTER_FILE = "data/Data/team_name_master.csv" # Re-including team master file
OUTPUT_DIR = "data/adjusted"
SUMMARY_FILE = "summaries/summary.txt"

# Set logging level to INFO by default
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure output and summary directories exist at the start
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
Path(SUMMARY_FILE).parent.mkdir(exist_ok=True)

# --- Shared utilities for normalization ---
def fix_team_name_format(name):
    """
    Adds a space between lowercase and uppercase letters (e.g., 'NewYork' -> 'New York').
    This is a preliminary step before more formal normalization.
    """
    name = str(name).strip()
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', name)

def get_normalized_team_name(name, team_name_map):
    """
    Normalizes a team name using a predefined mapping.
    If no direct match in the map, it attempts a simple title case as a fallback,
    and logs a warning for unmatched names.
    """
    original_name = str(name).strip()
    # Apply preliminary formatting and convert to lowercase for map lookup
    processed_name = fix_team_name_format(original_name).lower()

    # Try direct lookup in the lowercase map
    if processed_name in team_name_map:
        return team_name_map[processed_name] # Return the canonical name from the map

    # Fallback if no direct map match
    title_cased_name = original_name.title() # Use original name for title()
    logger.warning(f"⚠️ Team name '{original_name}' (processed as '{processed_name}') "
                   f"not found in master mapping. Falling back to '{title_cased_name}'. "
                   f"This may lead to unmatched batters/games.")
    return title_cased_name


def main():
    script_status = "PASS" # Default status, will change to WARN, ERROR, or SKIP
    home_count = 0
    away_count = 0
    unmatched_count = 0

    logger.info("📥 Loading input files...")
    try:
        batters = pd.read_csv(BATTERS_FILE)
        games = pd.read_csv(GAMES_FILE)
        team_master = pd.read_csv(TEAM_MASTER_FILE) # Re-loading team_master
    except FileNotFoundError as e:
        logger.error(f"❌ Critical Error: Input file not found: {e}. Ensure paths are correct.")
        script_status = "ERROR"
        with open(SUMMARY_FILE, "a") as f:
            f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {away_count} | Unmatched: {unmatched_count} (File not found: {e.name})\n")
        return
    except Exception as e:
        logger.error(f"❌ Error loading input files: {e}")
        script_status = "ERROR"
        with open(SUMMARY_FILE, "a") as f:
            f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {away_count} | Unmatched: {unmatched_count} (Loading error)\n")
        return

    # --- Initial Checks & Early Exits ---
    if batters.empty:
        logger.warning(f"⚠️ {BATTERS_FILE} is empty. No batters to process.")
        script_status = "SKIP"
        with open(SUMMARY_FILE, "a") as f:
            f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {away_count} | Unmatched: {unmatched_count} (No input batters)\n")
        return

    if games.empty:
        logger.warning(f"⚠️ {GAMES_FILE} is empty. Cannot determine home/away teams.")
        script_status = "SKIP"
        with open(SUMMARY_FILE, "a") as f:
            f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {home_count} | Unmatched: {len(batters)} (No game data)\n")
        return

    # --- Team Master Processing (Re-integrated) ---
    team_name_map = {}
    if team_master.empty:
        logger.warning(f"⚠️ {TEAM_MASTER_FILE} is empty. Team name normalization will rely only on `fix_team_name_format` and `title()` fallback, which is imprecise.")
    elif 'team_name' not in team_master.columns:
        logger.warning(f"⚠️ '{TEAM_MASTER_FILE}' does not contain a 'team_name' column. Team normalization will be ineffective.")
    else:
        # Create a lowercase mapping for robust lookups
        for team in team_master["team_name"].dropna().unique():
            # Store the canonical name (original case from master file) keyed by its lowercase version
            team_name_map[str(team).strip().lower()] = team

    if not team_name_map:
        logger.warning("⚠️ No valid teams found or mapped in team master file. Team normalization will be less effective.")


    # --- Column Checks ---
    if 'team' not in batters.columns:
        logger.error("❌ 'team' column missing in batters_today.csv. Please check the file.")
        raise ValueError("Missing 'team' column in batters_today.csv.") # Fatal error, raise
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        logger.error("❌ Missing 'home_team' or 'away_team' in games file. Please check the file.")
        raise ValueError("Missing 'home_team' or 'away_team' in games file.") # Fatal error, raise

    logger.info("🧽 Normalizing team names across all relevant DataFrames...")

    # Apply unified normalization using the get_normalized_team_name function
    batters['team'] = batters['team'].apply(lambda x: get_normalized_team_name(x, team_name_map))
    batters.drop_duplicates(inplace=True) # Drop duplicates after normalization

    games['home_team'] = games['home_team'].apply(lambda x: get_normalized_team_name(x, team_name_map))
    games['away_team'] = games['away_team'].apply(lambda x: get_normalized_team_name(x, team_name_map))

    home_teams = games['home_team'].unique().tolist()
    away_teams = games['away_team'].unique().tolist()
    all_game_teams = set(home_teams + away_teams)

    logger.info("🔀 Splitting batters into home and away groups...")
    # Filter batters that are playing today (i.e., their team is in all_game_teams)
    playing_batters = batters[batters['team'].isin(all_game_teams)].copy()

    home_batters = playing_batters[playing_batters['team'].isin(home_teams)].copy()
    home_batters["home_away"] = "home"

    away_batters = playing_batters[playing_batters['team'].isin(away_teams)].copy()
    away_batters["home_away"] = "away"

    # Identify truly unmatched batters (those not found in any game today)
    unmatched_batters = batters[~batters['team'].isin(all_game_teams)]
    unmatched_teams_summary = unmatched_batters["team"].value_counts().head(5).to_dict()

    home_count = len(home_batters)
    away_count = len(away_batters)
    unmatched_count = len(unmatched_batters)

    logger.info(f"✅ Processed {home_count} home batters and {away_count} away batters.")
    if unmatched_count > 0:
        logger.warning(f"❗ Skipped {unmatched_count} batters whose teams are not playing today or could not be matched.")
        if unmatched_teams_summary:
            logger.warning(f"⚠️ Top 5 unmatched team names: {unmatched_teams_summary}")

    # The row count check now specifically reflects if all *input* batters were accounted for
    total_processed_and_unmatched = home_count + away_count + unmatched_count
    if total_processed_and_unmatched == len(batters):
        logger.info(f"✅ All {len(batters)} input batters accounted for (Home: {home_count}, Away: {away_count}, Unmatched: {unmatched_count}).")
        # Status remains "PASS" if all original batters are categorized, even if some are unmatched.
    else:
        logger.error(f"❌ Mismatch in total rows: Input: {len(batters)}, Accounted: {total_processed_and_unmatched}. Data loss detected!")
        script_status = "ERROR" # This is a critical logic error if rows are truly lost.

    logger.info(f"💾 Saving output files to {OUTPUT_DIR}...")
    try:
        home_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_home.csv", index=False)
        away_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_away.csv", index=False)
        logger.info("✅ Output files 'batters_home.csv' and 'batters_away.csv' created.")
    except Exception as e:
        logger.error(f"❌ Error saving output files: {e}")
        script_status = "ERROR" # Update status if saving fails

    # --- Write to summary.txt ---
    with open(SUMMARY_FILE, "a") as f:
        f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {away_count} | Unmatched: {unmatched_count}\n")
    logger.info(f"📋 Summary written to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
