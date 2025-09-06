import pandas as pd
import logging
from pathlib import Path

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- File Paths ---
TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")
PITCHERS_HOME_FILE = Path("data/adjusted/pitchers_home.csv")
PITCHERS_AWAY_FILE = Path("data/adjusted/pitchers_away.csv")

# --- Helper Functions ---
def load_team_map():
    if not TEAM_MAP_FILE.exists():
        logger.critical(f"❌ Missing team mapping file: {TEAM_MAP_FILE}")
        raise FileNotFoundError(f"{TEAM_MAP_FILE} does not exist.")

    df = pd.read_csv(TEAM_MAP_FILE)
    required_cols = {"team_code", "team_name"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"❌ team_name_master.csv must contain columns: {required_cols}")

    df["team_code"] = df["team_code"].astype(str).str.strip()
    df["team_name"] = df["team_name"].astype(str).str.strip()
    return dict(zip(df["team_code"], df["team_name"]))

def update_team_names(pitchers_file: Path, team_map: dict):
    if not pitchers_file.exists():
        logger.warning(f"⚠️ File not found: {pitchers_file}")
        return

    df = pd.read_csv(pitchers_file)
    if "team" not in df.columns:
        raise ValueError(f"❌ Missing 'team' column in {pitchers_file}")

    df["team"] = df["team"].astype(str).str.strip()
    original_teams = set(df["team"])
    df["team"] = df["team"].map(team_map).fillna(df["team"])

    # Report unmapped teams
    mapped_teams = set(team_map.values())
    unmapped = [team for team in original_teams if team not in mapped_teams and team in df["team"].values]
    if unmapped:
        logger.warning(f"⚠️ Unmapped team codes in {pitchers_file.name}: {len(unmapped)}")
        for team in sorted(unmapped)[:5]:
            logger.debug(f"🔍 Unmapped team code: {team}")
    else:
        logger.info(f"✅ All teams mapped successfully in {pitchers_file.name}")

    df.to_csv(pitchers_file, index=False)
    logger.info(f"💾 Updated {len(df)} rows in {pitchers_file}")

# --- Main Execution ---
def main():
    team_map = load_team_map()
    update_team_names(PITCHERS_HOME_FILE, team_map)
    update_team_names(PITCHERS_AWAY_FILE, team_map)

if __name__ == "__main__":
    main()
