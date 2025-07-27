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

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Utilities ---
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
        logger.error(f"❌ Critical Error: Input file not found: {e}")
        return
    except Exception as e:
        logger.error(f"❌ Error loading input files: {e}")
        return

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
            f.write(f"split_batters_by_home_away: SKIP | Home: 0 | Away: 0 | Unmatched: {len(batters)} (No game data)\n")
        return

    if team_master.empty:
        logger.warning(f"⚠️ {TEAM_MASTER_FILE} is empty. Team name normalization may be incomplete.")

    valid_teams = team_master["team_name"].dropna().tolist()
    if not valid_teams:
        logger.warning("⚠️ No valid teams found in team master file.")

    if 'team' not in batters.columns:
        logger.error("❌ 'team' column missing in batters_today.csv.")
        raise ValueError("Missing 'team' column in batters_today.csv.")

    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        logger.error("❌ 'home_team' or 'away_team' missing in games file.")
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")

    # Normalize team names
    logger.info("🧽 Normalizing team names...")
    batters['team'] = batters['team'].apply(fix_team_name).apply(lambda x: normalize_team(x, valid_teams))
    batters.drop_duplicates(inplace=True)

    games['home_team'] = games['home_team'].astype(str).str.strip().str.title()
    games['away_team'] = games['away_team'].astype(str).str.strip().str.title()

    home_teams = games['home_team'].unique().tolist()
    away_teams = games['away_team'].unique().tolist()
    all_game_teams = set(home_teams + away_teams)

    logger.info("🔀 Splitting batters into home and away groups...")
    home_batters = batters[batters['team'].isin(home_teams)].copy()
    home_batters["home_away"] = "home"

    away_batters = batters[batters['team'].isin(away_teams)].copy()
    away_batters["home_away"] = "away"

    matched_teams = set(home_batters["team"]).union(set(away_batters["team"]))
    unmatched_batters = batters[~batters["team"].isin(matched_teams)]

    logger.info(f"✅ Found {len(home_batters)} home batters and {len(away_batters)} away batters.")
    logger.info(f"❗ Skipped {len(unmatched_batters)} unmatched batters.")

    total_out = len(home_batters) + len(away_batters)
    status = "PASS" if total_out == len(batters) else "FAIL"

    # --- Save Outputs ---
    logger.info(f"💾 Saving output files to {OUTPUT_DIR}...")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    out_home = Path(f"{OUTPUT_DIR}/batters_home.csv")
    out_away = Path(f"{OUTPUT_DIR}/batters_away.csv")

    try:
        home_batters.sort_values(by=["team", "name"]).to_csv(out_home, index=False)
        away_batters.sort_values(by=["team", "name"]).to_csv(out_away, index=False)
    except Exception as e:
        logger.error(f"❌ Error during file write: {e}")
        status = "ERROR"

    if out_home.exists():
        logger.info(f"✅ Output file '{out_home}' created.")
    else:
        logger.error(f"❌ Output file '{out_home}' NOT FOUND after write.")

    if out_away.exists():
        logger.info(f"✅ Output file '{out_away}' created.")
    else:
        logger.error(f"❌ Output file '{out_away}' NOT FOUND after write.")

    # --- Write to summary ---
    Path("summaries").mkdir(exist_ok=True)
    with open(SUMMARY_FILE, "a") as f:
        f.write(f"split_batters_by_home_away: {status} | Home: {len(home_batters)} | Away: {len(away_batters)} | Unmatched: {len(unmatched_batters)}\n")

    logger.info(f"📋 Summary written to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
