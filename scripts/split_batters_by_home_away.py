# scripts/split_batters_by_home_away.py

import pandas as pd
from pathlib import Path
import re
import logging

# --- Setup ---
BATTERS_FILE = "data/cleaned/batters_today.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MASTER_FILE = "data/Data/team_name_master.csv"
OUTPUT_DIR = "data/adjusted"
SUMMARY_FILE = "summaries/summary.txt"

# Set logging level to INFO by default
# For detailed debugging, you might temporarily set this to DEBUG:
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
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
    processed_name = fix_team_name_format(original_name).lower()

    if processed_name in team_name_map:
        return team_name_map[processed_name]
    else:
        title_cased_name = original_name.title()
        logger.warning(f"⚠️ Team name '{original_name}' (processed as '{processed_name}') "
                       f"not found in master mapping. Falling back to '{title_cased_name}'. "
                       f"This may lead to unmatched batters/games.")
        return title_cased_name


def main():
    script_status = "PASS"
    home_count = 0
    away_count = 0
    unmatched_count = 0

    logger.info("📥 Loading input files...")
    try:
        batters = pd.read_csv(BATTERS_FILE)
        games = pd.read_csv(GAMES_FILE)
        team_master = pd.read_csv(TEAM_MASTER_FILE)
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
    logger.info(f"Loaded {len(batters)} batters from {BATTERS_FILE}")


    if games.empty:
        logger.warning(f"⚠️ {GAMES_FILE} is empty. Cannot determine home/away teams.")
        script_status = "SKIP"
        with open(SUMMARY_FILE, "a") as f:
            f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {home_count} | Unmatched: {len(batters)} (No game data)\n")
        return
    logger.info(f"Loaded {len(games)} games from {GAMES_FILE}")


    # --- Team Master Processing ---
    team_name_map = {}
    if team_master.empty:
        logger.warning(f"⚠️ {TEAM_MASTER_FILE} is empty. Team name normalization will rely only on `fix_team_name_format` and `title()` fallback, which is imprecise.")
    elif 'team_name' not in team_master.columns:
        logger.warning(f"⚠️ '{TEAM_MASTER_FILE}' does not contain a 'team_name' column. Team normalization will be ineffective.")
    else:
        for team in team_master["team_name"].dropna().unique():
            team_name_map[str(team).strip().lower()] = team
        logger.info(f"Loaded {len(team_name_map)} valid team names from {TEAM_MASTER_FILE}")


    if not team_name_map:
        logger.warning("⚠️ No valid teams found or mapped in team master file. Team normalization will be less effective.")


    # --- Column Checks ---
    if 'team' not in batters.columns:
        logger.error("❌ 'team' column missing in batters_today.csv. Please check the file.")
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        logger.error("❌ Missing 'home_team' or 'away_team' in games file. Please check the file.")
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")

    logger.info("🧽 Normalizing team names across all relevant DataFrames...")

    # Log unique team names *before* normalization
    logger.debug(f"Batters teams BEFORE normalization: {batters['team'].unique().tolist()}")
    logger.debug(f"Games home teams BEFORE normalization: {games['home_team'].unique().tolist()}")
    logger.debug(f"Games away teams BEFORE normalization: {games['away_team'].unique().tolist()}")

    # Apply unified normalization
    batters['team'] = batters['team'].apply(lambda x: get_normalized_team_name(x, team_name_map))
    batters.drop_duplicates(inplace=True)

    games['home_team'] = games['home_team'].apply(lambda x: get_normalized_team_name(x, team_name_map))
    games['away_team'] = games['away_team'].apply(lambda x: get_normalized_team_name(x, team_name_map))

    # Log unique team names *after* normalization
    logger.debug(f"Batters teams AFTER normalization: {batters['team'].unique().tolist()}")
    logger.debug(f"Games home teams AFTER normalization: {games['home_team'].unique().tolist()}")
    logger.debug(f"Games away teams AFTER normalization: {games['away_team'].unique().tolist()}")

    home_teams = games['home_team'].unique().tolist()
    away_teams = games['away_team'].unique().tolist()
    all_game_teams = set(home_teams + away_teams)

    logger.info(f"Teams found in today's games: {list(all_game_teams)}")
    logger.info("🔀 Splitting batters into home and away groups...")

    # Identify potential mismatches by comparing sets of teams
    batter_unique_teams = set(batters['team'].unique())
    game_unique_teams = set(all_game_teams)

    teams_in_batters_not_in_games = batter_unique_teams - game_unique_teams
    if teams_in_batters_not_in_games:
        logger.warning(f"⚠️ Teams present in 'batters' file but NOT in 'games' file after normalization: {list(teams_in_batters_not_in_games)}")

    teams_in_games_not_in_batters = game_unique_teams - batter_unique_teams
    if teams_in_games_not_in_batters:
        logger.warning(f"⚠️ Teams present in 'games' file but NO batters found for them: {list(teams_in_games_not_in_batters)}")


    # Filter batters that are playing today (i.e., their team is in all_game_teams)
    playing_batters = batters[batters['team'].isin(all_game_teams)].copy()
    logger.info(f"Found {len(playing_batters)} batters whose teams are playing today.")
    if playing_batters.empty and len(batters) > 0:
        logger.error("❌ No batters matched to any game team! This is likely a major normalization issue.")


    home_batters = playing_batters[playing_batters['team'].isin(home_teams)].copy()
    home_batters["home_away"] = "home"
    logger.debug(f"Home teams for split: {home_teams}")
    logger.debug(f"Home batters (first 5 rows):\n{home_batters.head()}")


    away_batters = playing_batters[playing_batters['team'].isin(away_teams)].copy()
    away_batters["home_away"] = "away"
    logger.debug(f"Away teams for split: {away_teams}")
    logger.debug(f"Away batters (first 5 rows):\n{away_batters.head()}")


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
    else:
        logger.error(f"❌ Mismatch in total rows: Input: {len(batters)}, Accounted: {total_processed_and_unmatched}. Data loss detected!")
        script_status = "ERROR"

    logger.info(f"💾 Saving output files to {OUTPUT_DIR}...")
    try:
        if not home_batters.empty:
            home_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_home.csv", index=False)
            logger.info("✅ Output file 'batters_home.csv' created.")
        else:
            logger.warning("⚠️ 'batters_home.csv' not created as no home batters were found.")

        if not away_batters.empty:
            away_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_away.csv", index=False)
            logger.info("✅ Output file 'batters_away.csv' created.")
        else:
            logger.warning("⚠️ 'batters_away.csv' not created as no away batters were found.")

    except Exception as e:
        logger.error(f"❌ Error saving output files: {e}")
        script_status = "ERROR"

    # --- Write to summary.txt ---
    with open(SUMMARY_FILE, "a") as f:
        f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {away_count} | Unmatched: {unmatched_count}\n")
    logger.info(f"📋 Summary written to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
