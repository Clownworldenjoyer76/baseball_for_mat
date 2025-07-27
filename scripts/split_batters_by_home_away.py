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

# Logging config - Changed to DEBUG for detailed output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure output and summary directories exist at the start
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
Path(SUMMARY_FILE).parent.mkdir(exist_ok=True)

# --- Utilities ---
def fix_team_name(name):
    """
    Adds a space between lowercase and uppercase letters (e.g., 'NewYork' -> 'New York').
    """
    name = str(name).strip()
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', name)

def normalize_team(name, team_name_map):
    """
    Normalizes a team name using a predefined mapping.
    Uses map if available, falls back to title case.
    """
    original_name = str(name).strip()
    processed_name = fix_team_name(original_name).lower()

    if processed_name in team_name_map:
        logger.debug(f"Normalized '{original_name}' to '{team_name_map[processed_name]}' using map.")
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
        logger.info(f"Loaded {len(batters)} rows from {BATTERS_FILE}")
        logger.info(f"Loaded {len(games)} rows from {GAMES_FILE}")
        logger.info(f"Loaded {len(team_master)} rows from {TEAM_MASTER_FILE}")
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

    # --- Team Master Processing ---
    team_name_map = {}
    if team_master.empty:
        logger.warning(f"⚠️ {TEAM_MASTER_FILE} is empty. Team name normalization will rely only on `fix_team_name` and `title()` fallback, which is imprecise.")
    elif 'team_name' not in team_master.columns:
        logger.warning(f"⚠️ '{TEAM_MASTER_FILE}' does not contain a 'team_name' column. Team normalization will be ineffective.")
    else:
        for team in team_master["team_name"].dropna().unique():
            # Store the canonical name (original case from master file) keyed by its lowercase version
            team_name_map[str(team).strip().lower()] = team
        logger.info(f"Constructed team_name_map with {len(team_name_map)} entries.")
        logger.debug(f"Team Master Map Sample (first 5): {dict(list(team_name_map.items())[:5])}")


    if not team_name_map and not team_master.empty and 'team_name' in team_master.columns:
        # This means the file was not empty and had the column, but no valid teams were extracted
        logger.error("❌ Failed to create valid team name map despite master file existing. Check master file content.")
        script_status = "ERROR"
        with open(SUMMARY_FILE, "a") as f:
            f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {away_count} | Unmatched: {len(batters)} (Team map error)\n")
        return


    # --- Column Checks ---
    if 'team' not in batters.columns:
        logger.error("❌ 'team' column missing in batters_today.csv. Please check the file.")
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        logger.error("❌ Missing 'home_team' or 'away_team' in games file. Please check the file.")
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")


    logger.info("🧽 Normalizing team names across all relevant DataFrames...")

    # DEBUG: Log unique team names *before* normalization
    logger.debug(f"Batters unique teams BEFORE normalization ({len(batters['team'].unique())} unique): {sorted(batters['team'].unique().tolist())}")
    logger.debug(f"Games unique home teams BEFORE normalization ({len(games['home_team'].unique())} unique): {sorted(games['home_team'].unique().tolist())}")
    logger.debug(f"Games unique away teams BEFORE normalization ({len(games['away_team'].unique())} unique): {sorted(games['away_team'].unique().tolist())}")


    # Apply normalization to batters
    batters['team'] = batters['team'].apply(fix_team_name).apply(lambda x: normalize_team(x, team_name_map))
    batters.drop_duplicates(inplace=True)
    logger.debug(f"Batters DataFrame after normalization and de-duplication: {len(batters)} rows.")

    # Apply normalization to games
    # IMPORTANT: Your `games` normalization was different from `batters`.
    # It used .astype(str).str.strip().str.title() directly.
    # To ensure consistency, it should also use your `normalize_team` function.
    games['home_team'] = games['home_team'].apply(fix_team_name).apply(lambda x: normalize_team(x, team_name_map))
    games['away_team'] = games['away_team'].apply(fix_team_name).apply(lambda x: normalize_team(x, team_name_map))

    # DEBUG: Log unique team names *after* normalization
    logger.debug(f"Batters unique teams AFTER normalization ({len(batters['team'].unique())} unique): {sorted(batters['team'].unique().tolist())}")
    logger.debug(f"Games unique home teams AFTER normalization ({len(games['home_team'].unique())} unique): {sorted(games['home_team'].unique().tolist())}")
    logger.debug(f"Games unique away teams AFTER normalization ({len(games['away_team'].unique())} unique): {sorted(games['away_team'].unique().tolist())}")


    home_teams = games['home_team'].unique().tolist()
    away_teams = games['away_team'].unique().tolist()
    all_game_teams = set(home_teams + away_teams)
    logger.info(f"Total unique teams identified in today's games: {len(all_game_teams)}")
    logger.debug(f"All game teams: {sorted(list(all_game_teams))}")


    # DEBUG: Compare the sets of teams
    batter_teams_set = set(batters['team'].unique())
    logger.debug(f"Unique teams in batters after normalization: {sorted(list(batter_teams_set))}")

    teams_in_batters_not_in_games = batter_teams_set - all_game_teams
    if teams_in_batters_not_in_games:
        logger.warning(f"⚠️ Teams present in 'batters' but NOT found in 'games' after normalization: {sorted(list(teams_in_batters_not_in_games))}")
    else:
        logger.info("✅ All batter teams are present in today's games (or no batters found).")

    teams_in_games_not_in_batters = all_game_teams - batter_teams_set
    if teams_in_games_not_in_batters:
        logger.warning(f"⚠️ Teams present in 'games' but NO batters found for them: {sorted(list(teams_in_games_not_in_batters))}")
    else:
        logger.info("✅ All game teams have corresponding batters (or no games found).")


    logger.info("🔀 Splitting batters into home and away groups...")
    # Filter batters that are playing today (i.e., their team is in all_game_teams)
    playing_batters = batters[batters['team'].isin(all_game_teams)].copy()
    logger.info(f"Initially identified {len(playing_batters)} batters as playing today.")
    if playing_batters.empty and not batters.empty:
        logger.error("❌ After filtering for playing teams, 'playing_batters' is empty! This is the core issue for empty output files.")
        logger.error("   This means no team names in 'batters' matched any team name in 'games' after normalization.")


    home_batters = playing_batters[playing_batters['team'].isin(home_teams)].copy()
    home_batters["home_away"] = "home"
    home_count = len(home_batters)
    logger.info(f"Determined {home_count} home batters.")
    if home_batters.empty:
        logger.debug("No home batters found.")
    else:
        logger.debug(f"Home batters (first 5 rows):\n{home_batters.head().to_string()}") # .to_string() for better console formatting


    away_batters = playing_batters[playing_batters['team'].isin(away_teams)].copy()
    away_batters["home_away"] = "away"
    away_count = len(away_batters)
    logger.info(f"Determined {away_count} away batters.")
    if away_batters.empty:
        logger.debug("No away batters found.")
    else:
        logger.debug(f"Away batters (first 5 rows):\n{away_batters.head().to_string()}")


    # Identify truly unmatched batters (those not found in any game today)
    unmatched_batters = batters[~batters['team'].isin(all_game_teams)]
    unmatched_count = len(unmatched_batters)
    unmatched_teams_summary = unmatched_batters["team"].value_counts().head(5).to_dict()

    logger.info(f"✅ Processed {home_count} home batters and {away_count} away batters.")
    if unmatched_count > 0:
        logger.warning(f"❗ Skipped {unmatched_count} batters whose teams are not playing today or could not be matched after normalization.")
        if unmatched_teams_summary:
            logger.warning(f"⚠️ Top 5 unmatched team names: {unmatched_teams_summary}")
    else:
        logger.info("✅ No unmatched batters detected (all batters were either home or away).")

    # The row count check now specifically reflects if all *input* batters were accounted for
    total_processed_and_unmatched = home_count + away_count + unmatched_count
    if total_processed_and_unmatched == len(batters):
        logger.info(f"✅ All {len(batters)} input batters accounted for (Home: {home_count}, Away: {away_count}, Unmatched: {unmatched_count}).")
        script_status = "PASS" if home_count > 0 or away_count > 0 else "WARN" # If all accounted but no home/away, warn.
    else:
        logger.error(f"❌ Mismatch in total rows: Input: {len(batters)}, Accounted: {total_processed_and_unmatched}. Data loss detected!")
        script_status = "ERROR"


    logger.info(f"💾 Saving output files to {OUTPUT_DIR}...")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True) # Ensure directory exists (redundant but safe)

    out_home_path = Path(f"{OUTPUT_DIR}/batters_home.csv")
    out_away_path = Path(f"{OUTPUT_DIR}/batters_away.csv")

    try:
        if not home_batters.empty:
            home_batters.sort_values(by=["team", "name"]).to_csv(out_home_path, index=False)
            logger.info(f"✅ Output file '{out_home_path}' created with {len(home_batters)} rows.")
        else:
            logger.warning(f"⚠️ {out_home_path} not created as no home batters were found. File might be created with only headers.")
            # Still create the file with headers if empty, as per original behavior of to_csv
            home_batters.to_csv(out_home_path, index=False)


        if not away_batters.empty:
            away_batters.sort_values(by=["team", "name"]).to_csv(out_away_path, index=False)
            logger.info(f"✅ Output file '{out_away_path}' created with {len(away_batters)} rows.")
        else:
            logger.warning(f"⚠️ {out_away_path} not created as no away batters were found. File might be created with only headers.")
            # Still create the file with headers if empty
            away_batters.to_csv(out_away_path, index=False)

    except Exception as e:
        logger.error(f"❌ Error saving output files: {e}")
        script_status = "ERROR"

    # --- Write to summary ---
    Path("summaries").mkdir(exist_ok=True) # Ensure directory exists
    with open(SUMMARY_FILE, "a") as f:
        f.write(f"split_batters_by_home_away: {script_status} | Home: {home_count} | Away: {away_count} | Unmatched: {unmatched_count}\n")

    logger.info(f"📋 Summary written to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
