import pandas as pd
from pathlib import Path
import re
import logging

# --- Setup ---
BATTERS_FILE = "data/cleaned/batters_today.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MASTER_FILE = "data/Data/team_name_master.csv"
OUTPUT_DIR = "data/adjusted"

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

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    parts = name.strip().replace(".", "").split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

def main():
    logger.info("📥 Loading input files...")
    batters = pd.read_csv(BATTERS_FILE)
    games = pd.read_csv(GAMES_FILE)
    team_master = pd.read_csv(TEAM_MASTER_FILE)
    valid_teams = team_master["team_name"].dropna().tolist()

    if 'team' not in batters.columns:
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")

    # --- Normalize and deduplicate ---
    logger.info("🧽 Normalizing team and player name formats...")
    batters['team'] = batters['team'].apply(fix_team_name).apply(lambda x: normalize_team(x, valid_teams))
    batters['name'] = batters['name'].apply(normalize_name)
    batters.drop_duplicates(inplace=True)

    games['home_team'] = games['home_team'].astype(str).str.strip().str.title()
    games['away_team'] = games['away_team'].astype(str).str.strip().str.title()

    home_teams = games['home_team'].unique().tolist()
    away_teams = games['away_team'].unique().tolist()
    all_game_teams = set(home_teams + away_teams)

    # --- Split batters ---
    logger.info("🔀 Splitting batters into home and away groups...")
    home_batters = batters[batters['team'].isin(home_teams)].copy()
    home_batters["home_away"] = "home"

    away_batters = batters[batters['team'].isin(away_teams)].copy()
    away_batters["home_away"] = "away"

    matched_teams = set(home_batters["team"]).union(set(away_batters["team"]))
    unmatched_batters = batters[~batters["team"].isin(matched_teams)]
    unmatched_teams = unmatched_batters["team"].value_counts().head(5).to_dict()

    # --- Logging and Validation ---
    logger.info(f"✅ Saved {len(home_batters)} home batters and {len(away_batters)} away batters")
    logger.info(f"❗ Skipped {len(unmatched_batters)} unmatched batters from teams not found in today's games")
    if unmatched_teams:
        logger.warning(f"⚠️ Top 5 unmatched team names: {unmatched_teams}")
    all_teams_matched = batters["team"].isin(all_game_teams).all()
    if not all_teams_matched:
        logger.warning("⚠️ Some teams in batters_today.csv did not match any home or away team in todaysgames_normalized.csv")

    total_out = len(home_batters) + len(away_batters)
    if total_out != len(batters):
        logger.error(f"❌ Mismatch: total output ({total_out}) != input rows ({len(batters)})")
    else:
        logger.info("✅ Row count integrity confirmed.")

    # --- Save Outputs ---
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    home_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_home.csv", index=False)
    away_batters.sort_values(by=["team", "name"]).to_csv(f"{OUTPUT_DIR}/batters_away.csv", index=False)

if __name__ == "__main__":
    main()
