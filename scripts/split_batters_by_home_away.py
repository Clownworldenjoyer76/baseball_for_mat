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

# Set logging level to INFO by default, but you can change it to DEBUG for more verbosity
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Shared utilities ---
def fix_team_name(name):
    name = str(name).strip()
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', name)

def normalize_team(name, valid_teams):
    name = str(name).strip().lower()
    for valid in valid_teams:
        if name == valid.lower():
            return valid
    return name.title()

def main():
    logger.info("📥 Loading input files...")
    try:
        batters = pd.read_csv(BATTERS_FILE)
        games = pd.read_csv(GAMES_FILE)
        team_master = pd.read_csv(TEAM_MASTER_FILE)
    except FileNotFoundError as e:
        logger.error(f"❌ Critical Error: Input file not found: {e}. Ensure paths are correct.")
        return # Exit if critical files are missing
    except Exception as e:
        logger.error(f"❌ Error loading input files: {e}")
        return

    # Debug: Check if DataFrames are empty right after loading
    if batters.empty:
        logger.warning(f"⚠️ {BATTERS_FILE} is empty. No batters to process.")
        Path("summaries").mkdir(exist_ok=True)
        with open(SUMMARY_FILE, "a") as f:
            f.write("split_batters_by_home_away: SKIP | Home: 0 | Away: 0 | Unmatched: 0 (No input batters)\n")
        return
    if games.empty:
        logger.warning(f"⚠️ {GAMES_FILE} is empty. Cannot determine home/away teams.")
        Path("summaries").mkdir(exist_ok=True)
        with open(SUMMARY_FILE, "a") as f:
            f.write("split_batters_by_home_away: SKIP | Home: 0 | Away: 0 | Unmatched: {len(batters)} (No game data)\n")
        return
    if team_master.empty:
        logger.warning(f"⚠️ {TEAM_MASTER_FILE} is empty. Team name normalization may be incomplete.")
        # Script can still proceed, but normalization might be less effective

    valid_teams = team_master["team_name"].dropna().tolist()
    if not valid_teams:
        logger.warning("⚠️ No valid teams found in team master file. Team normalization might be ineffective.")

    if 'team' not in batters.columns:
        logger.error("❌ 'team' column missing in batters_today.csv. Please check the file.")
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        logger.error("❌ Missing 'home_team' or 'away_team' in games file. Please check the file.")
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")

    logger.debug(f"DEBUG: Initial batters count: {len(batters)}")
    logger.debug(f"DEBUG: Initial games count: {len(games)}")

    # --- Normalize team names ---
    logger.info("🧽 Normalizing team names...")
    batters['team'] = batters['team'].apply(fix_team_name).apply(lambda x: normalize_team(x, valid_teams))
    batters.drop_duplicates(inplace=True)
    logger.debug(f"DEBUG: Batters count after normalization and deduplication: {len(batters)}")
    logger.debug(f"DEBUG: Sample normalized batters teams: {batters['team'].value_counts().head(3).to_dict()}")

    games['home_team'] = games['home_team'].astype(str).str.strip().str.title()
    games['away_team'] = games['away_team'].astype(str).str.strip().str.title()

    home_teams = games['home_team'].unique().tolist()
    away_teams = games['away_team'].unique().tolist()
    all_game_teams = set(home_teams + away_teams)

    logger.debug(f"DEBUG: Unique home teams from games: {home_teams}")
    logger.debug(f"DEBUG: Unique away teams from games: {away_teams}")

    # --- Split batters ---
    logger.info("🔀 Splitting batters into home and away groups...")
    home_batters = batters[batters['team'].isin(home_teams)].copy()
    home_batters["home_away"] = "home"

    away_batters = batters[batters['team'].isin(away_teams)].copy()
    away_batters["home_away"] = "away"

    matched_teams = set(home_batters["team"]).union(set(away_batters["team"]))
    unmatched_batters = batters[~batters["team"].isin(matched_teams)]
    unmatched_teams = unmatched_batters["team"].value_counts().head(5).to_dict()

    # --- Logging ---
    logger.info(f"✅ Saved {len(home_batters)} home batters and {len(away_batters)} away batters")
    logger.info(f"❗ Skipped {len(unmatched_batters)} unmatched batters")
    if unmatched_teams:
        logger.warning(f"⚠️ Top 5 unmatched team names: {unmatched_teams}")
    all_teams_matched = batters["team"].isin(all_game_teams).all()
    if not all_teams_matched:
        logger.warning("⚠️ Some batters had teams not in today's games")

    total_out = len(home_batters) + len(away_batters)
    status = "PASS" if total_out == len(batters) else "FAIL"
    logger.info(f"✅ Row count check: {status}")
    if status == "FAIL":
        logger.warning(f"⚠️ Mismatch in total rows: Input: {len(batters)}, Output: {total_out}. Check for unmatched teams.")

    # --- Save Outputs ---
    logger.info(f"💾 Saving output files to {OUTPUT_DIR}...")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    try:
        home_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_home.csv", index=False)
        away_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_away.csv", index=False)
        logger.info("✅ Output files 'batters_home.csv' and 'batters_away.csv' created.")
    except Exception as e:
        logger.error(f"❌ Error saving output files: {e}")
        status = "ERROR" # Update status if saving fails

    # --- Write to summary.txt ---
    Path("summaries").mkdir(exist_ok=True)
    with open(SUMMARY_FILE, "a") as f:
        f.write(f"split_batters_by_home_away: {status} | Home: {len(home_batters)} | Away: {len(away_batters)} | Unmatched: {len(unmatched_batters)}\n")
    logger.info(f"📋 Summary written to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
