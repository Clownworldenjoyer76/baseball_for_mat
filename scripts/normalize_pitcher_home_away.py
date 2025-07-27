import pandas as pd
import logging
from pathlib import Path
import shutil # For copying files
import os     # For creating directories

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Original Input File Paths ---
PITCHERS_FILE_SOURCE = Path("data/cleaned/pitchers_normalized_cleaned.csv")
GAMES_FILE_SOURCE = Path("data/raw/todaysgames_normalized.csv")
TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")

# --- New Processing/Output Directory ---
# This is where copies of inputs will go, and where final outputs will be saved.
PROCESSING_DIR = Path("data/final_processed_pitchers")

# --- Paths for files within the new PROCESSING_DIR ---
PITCHERS_FILE_TEMP = PROCESSING_DIR / "pitchers_normalized_cleaned.csv"
GAMES_FILE_TEMP = PROCESSING_DIR / "todaysgames_normalized.csv"
OUT_HOME = PROCESSING_DIR / "pitchers_home.csv"
OUT_AWAY = PROCESSING_DIR / "pitchers_away.csv"

# --- Helper Functions for Team Mapping ---
def load_team_map():
    """Loads the team name mapping from a CSV file."""
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

# --- Original Helper Functions (modified to use temp paths) ---
def load_pitchers():
    """Loads and preprocesses the pitchers data from the temp location."""
    if not PITCHERS_FILE_TEMP.exists():
        logger.critical(f"❌ Missing temporary pitcher file: {PITCHERS_FILE_TEMP}")
        raise FileNotFoundError(f"{PITCHERS_FILE_TEMP} does not exist. Ensure it was copied.")

    df = pd.read_csv(PITCHERS_FILE_TEMP)
    df["name"] = df["last_name"].astype(str).str.strip() + ", " + df["first_name"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["name", "team"])
    return df

def filter_and_tag(pitchers_df: pd.DataFrame, side: str, team_map: dict):
    """
    Filters and tags pitchers as home or away, applying team name standardization.

    Args:
        pitchers_df (pd.DataFrame): DataFrame containing pitcher data.
        side (str): 'home' or 'away' to specify which pitcher/team to filter for.
        team_map (dict): A dictionary for mapping team codes to full names.

    Returns:
        tuple: A tuple containing the filtered DataFrame, a list of missing pitchers,
               and a list of unmatched teams.
    """
    key = f"pitcher_{side}"
    team_key = f"{side}_team"

    tagged = []
    missing = []
    unmatched_teams = []

    # Load games file from the TEMP location
    if not GAMES_FILE_TEMP.exists():
        logger.critical(f"❌ Missing temporary games file: {GAMES_FILE_TEMP}")
        raise FileNotFoundError(f"{GAMES_FILE_TEMP} does not exist. Ensure it was copied.")

    full_games_df = pd.read_csv(GAMES_FILE_TEMP)[["pitcher_home", "pitcher_away", "home_team", "away_team"]]
    full_games_df[key] = full_games_df[key].astype(str).str.strip() # Clean pitcher names

    # Apply standardization to game team names *before* matching
    full_games_df['home_team'] = full_games_df['home_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['home_team'])
    full_games_df['away_team'] = full_games_df['away_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['away_team'])
    full_games_df[team_key] = full_games_df[team_key].astype(str).str.strip() # Ensure 'team_key' column is also clean after map

    for idx, row in full_games_df.iterrows():
        pitcher_name = row[key]
        game_home_team = row['home_team'] # Standardized game home team
        game_away_team = row['away_team'] # Standardized game away team

        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()

        if matched.empty:
            missing.append(pitcher_name)
            unmatched_teams.append(row[team_key])
            logger.debug(f"🔍 Pitcher '{pitcher_name}' (Team: {row[team_key]}, Game Index: {idx}) not found in copied pitcher file.")
        else:
            matched["team"] = row[team_key] # Pitcher's own team in the game context (standardized)
            matched["home_away"] = side
            matched["game_home_team"] = game_home_team # Full game context
            matched["game_away_team"] = game_away_team # Full game context
            tagged.append(matched)
            logger.debug(f"✅ Matched pitcher '{pitcher_name}' for {side} team '{row[team_key]}'.")

    if tagged:
        df = pd.concat(tagged, ignore_index=True)
        df.drop(columns=[col for col in df.columns if col.endswith(".1")], errors='ignore', inplace=True)
        df.sort_values(by=["team", "name"], inplace=True)
        df.drop_duplicates(inplace=True)

        # Final standardization pass on the 'team' column if pitchers_df wasn't perfectly clean initially
        df["team"] = df["team"].astype(str).str.strip().map(team_map).fillna(df["team"])

        logger.info(f"Generated {len(df)} {side} pitcher records.")
        return df, missing, unmatched_teams

    logger.warning(f"⚠️ No {side} pitchers found or matched.")
    # Return an empty DataFrame with expected columns if no matches
    return pd.DataFrame(columns=pitchers_df.columns.tolist() + ["team", "home_away", "game_home_team", "game_away_team"]), missing, unmatched_teams

# --- Main Execution ---
def main():
    logger.info("Starting pitcher data processing and standardization with new workflow.")

    # 1. Create the new processing directory if it doesn't exist
    os.makedirs(PROCESSING_DIR, exist_ok=True)
    logger.info(f"Ensured processing directory exists: {PROCESSING_DIR}")

    # 2. Copy input files to the new location
    try:
        shutil.copy(PITCHERS_FILE_SOURCE, PITCHERS_FILE_TEMP)
        logger.info(f"Copied {PITCHERS_FILE_SOURCE.name} to {PITCHERS_FILE_TEMP}")
        shutil.copy(GAMES_FILE_SOURCE, GAMES_FILE_TEMP)
        logger.info(f"Copied {GAMES_FILE_SOURCE.name} to {GAMES_FILE_TEMP}")
    except FileNotFoundError as e:
        logger.critical(f"❌ Failed to copy source file: {e}")
        logger.critical("Please ensure input files exist at their source paths.")
        return # Exit if files can't be copied

    team_map = load_team_map() # Load the map once
    logger.info(f"Loaded team map with {len(team_map)} entries.")

    pitchers_df = load_pitchers() # Load from the temp location
    logger.info(f"Loaded {len(pitchers_df)} unique pitchers from {PITCHERS_FILE_TEMP}.")

    # Process and get dataframes
    home_df, home_missing, home_unmatched_teams = filter_and_tag(pitchers_df, "home", team_map)
    away_df, away_missing, away_unmatched_teams = filter_and_tag(pitchers_df, "away", team_map)

    # 3. Save output files to the new location
    home_df.to_csv(OUT_HOME, index=False)
    away_df.to_csv(OUT_AWAY, index=False)

    logger.info(f"✅ Wrote {len(home_df)} rows to {OUT_HOME}")
    logger.info(f"✅ Wrote {len(away_df)} rows to {OUT_AWAY}")

    # Validation and reporting (using the copied games file for comparison)
    raw_games_df = pd.read_csv(GAMES_FILE_TEMP)
    expected = len(raw_games_df) * 2
    actual = len(home_df) + len(away_df)
    if actual != expected:
        logger.warning(f"⚠️ Mismatch: Expected {expected} total pitchers from {GAMES_FILE_TEMP}, but got {actual}")
    else:
        logger.info(f"Total pitchers generated ({actual}) matches expected count ({expected}).")

    if home_missing:
        logger.warning(f"\n=== MISSING HOME PITCHERS ({len(set(home_missing))} unique) ===")
        for name in sorted(set(home_missing)):
            logger.warning(f"  - {name}")

    if away_missing:
        logger.warning(f"\n=== MISSING AWAY PITCHERS ({len(set(away_missing))} unique) ===")
        for name in sorted(set(away_missing)):
            logger.warning(f"  - {name}")

    unmatched = set(home_unmatched_teams + away_unmatched_teams)
    if unmatched:
        logger.warning(f"\n⚠️ Unmatched team codes from copied GAMES_FILE (after initial map attempt): {len(unmatched)}")
        logger.warning("Top 5 unmatched teams:")
        for team in sorted(unmatched)[:5]:
            logger.warning(f"  - {team}")

    logger.info("Pitcher data processing and standardization completed.")

if __name__ == "__main__":
    main()
