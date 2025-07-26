import pandas as pd
import logging

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BATTERS_FILE = "data/cleaned/batters_today.csv"
TEAM_MASTER_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/cleaned/batters_today.csv"

def main():
    logger.info("📥 Loading batters_today and team_name_master...")
    batters = pd.read_csv(BATTERS_FILE)
    team_map = pd.read_csv(TEAM_MASTER_FILE)

    if 'team' not in batters.columns:
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'team_code' not in team_map.columns or 'team_name' not in team_map.columns:
        raise ValueError("Missing 'team_code' or 'team_name' in team_name_master.csv.")

    team_dict = dict(zip(team_map['team_code'], team_map['team_name']))

    logger.info("🔁 Replacing team codes with official team names...")
    before_unmapped = batters['team'].isna().sum()
    batters['team'] = batters['team'].map(team_dict)
    after_unmapped = batters['team'].isna().sum()

    if after_unmapped > 0:
        unmapped_pct = (after_unmapped / len(batters)) * 100
        if unmapped_pct > 5:
            logger.warning(f"⚠️ {after_unmapped} teams ({unmapped_pct:.2f}%) could not be mapped to official names.")
        else:
            logger.info(f"ℹ️ {after_unmapped} teams ({unmapped_pct:.2f}%) could not be mapped. Proceeding with partial fill.")

    # Fill unmapped values with original values
    batters['team'] = batters['team'].fillna(batters['team'])

    logger.info(f"✅ Normalized teams in {len(batters)} rows. Saving to {OUTPUT_FILE}...")
    batters.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
