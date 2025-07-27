import pandas as pd
import logging
from pathlib import Path
import sys # Import sys for command-line arguments
import shutil # For potential cleanup, though YAML will manage temp inputs
import os

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Fixed File Path (Team Map is always from source) ---
TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")

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

# --- Core Processing Function ---
def process_pitcher_data(pitchers_input_path: Path, games_input_path: Path,
                         output_home_path: Path, output_away_path: Path):
    """
    Main function to load, process, and save pitcher data.
    Takes all paths as arguments.
    """
    logger.info("Starting pitcher data processing and standardization.")

    team_map = load_team_map()
    logger.info(f"Loaded team map with {len(team_map)} entries.")

    # Load pitchers data from the specified input path
    if not pitchers_input_path.exists():
        logger.critical(f"❌ Missing pitcher input file: {pitchers_input_path}")
        raise FileNotFoundError(f"{pitchers_input_path} does not exist.")
    pitchers_df = pd.read_csv(pitchers_input_path)
    pitchers_df["name"] = pitchers_df["last_name"].astype(str).str.strip() + ", " + pitchers_df["first_name"].astype(str).str.strip()
    pitchers_df = pitchers_df.drop_duplicates(subset=["name", "team"])
    logger.info(f"Loaded {len(pitchers_df)} unique pitchers from {pitchers_input_path}.")

    # Load games data from the specified input path
    if not games_input_path.exists():
        logger.critical(f"❌ Missing games input file: {games_input_path}")
        raise FileNotFoundError(f"{games_input_path} does not exist.")
    full_games_df = pd.read_csv(games_input_path)[["pitcher_home", "pitcher_away", "home_team", "away_team"]]
    logger.info(f"Loaded {len(full_games_df)} games from {games_input_path}.")

    # Apply standardization to game team names immediately
    full_games_df['home_team'] = full_games_df['home_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['home_team'])
    full_games_df['away_team'] = full_games_df['away_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['away_team'])

    # --- Process Home Pitchers ---
    home_tagged = []
    home_missing_pitchers = []
    home_unmatched_teams = []

    for idx, row in full_games_df.iterrows():
        pitcher_name = row['pitcher_home']
        game_home_team = row['home_team']
        game_away_team = row['away_team']

        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()

        if matched.empty:
            home_missing_pitchers.append(pitcher_name)
            home_unmatched_teams.append(game_home_team) # Team of the missing pitcher
            logger.debug(f"🔍 Home pitcher '{pitcher_name}' (Team: {game_home_team}, Game Index: {idx}) not found.")
        else:
            matched["team"] = game_home_team # This pitcher's own team
            matched["home_away"] = "home"
            matched["game_home_team"] = game_home_team # Full game context
            matched["game_away_team"] = game_away_team # Full game context
            home_tagged.append(matched)
            logger.debug(f"✅ Matched home pitcher '{pitcher_name}' for team '{game_home_team}'.")

    if home_tagged:
        home_df = pd.concat(home_tagged, ignore_index=True)
        home_df.drop(columns=[col for col in home_df.columns if col.endswith(".1")], errors='ignore', inplace=True)
        home_df.sort_values(by=["team", "name"], inplace=True)
        home_df.drop_duplicates(inplace=True)
        home_df["team"] = home_df["team"].astype(str).str.strip().map(team_map).fillna(home_df["team"])
        logger.info(f"Generated {len(home_df)} home pitcher records.")
    else:
        home_df = pd.DataFrame(columns=pitchers_df.columns.tolist() + ["name", "team", "home_away", "game_home_team", "game_away_team"])
        logger.warning("⚠️ No home pitchers found or matched.")

    # --- Process Away Pitchers ---
    away_tagged = []
    away_missing_pitchers = []
    away_unmatched_teams = []

    for idx, row in full_games_df.iterrows():
        pitcher_name = row['pitcher_away']
        game_home_team = row['home_team']
        game_away_team = row['away_team']

        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()

        if matched.empty:
            away_missing_pitchers.append(pitcher_name)
            away_unmatched_teams.append(game_away_team) # Team of the missing pitcher
            logger.debug(f"🔍 Away pitcher '{pitcher_name}' (Team: {game_away_team}, Game Index: {idx}) not found.")
        else:
            matched["team"] = game_away_team # This pitcher's own team
            matched["home_away"] = "away"
            matched["game_home_team"] = game_home_team # Full game context
            matched["game_away_team"] = game_away_team # Full game context
            away_tagged.append(matched)
            logger.debug(f"✅ Matched away pitcher '{pitcher_name}' for team '{game_away_team}'.")

    if away_tagged:
        away_df = pd.concat(away_tagged, ignore_index=True)
        away_df.drop(columns=[col for col in away_df.columns if col.endswith(".1")], errors='ignore', inplace=True)
        away_df.sort_values(by=["team", "name"], inplace=True)
        away_df.drop_duplicates(inplace=True)
        away_df["team"] = away_df["team"].astype(str).str.strip().map(team_map).fillna(away_df["team"])
        logger.info(f"Generated {len(away_df)} away pitcher records.")
    else:
        away_df = pd.DataFrame(columns=pitchers_df.columns.tolist() + ["name", "team", "home_away", "game_home_team", "game_away_team"])
        logger.warning("⚠️ No away pitchers found or matched.")

    # Ensure output directories exist
    os.makedirs(output_home_path.parent, exist_ok=True)
    os.makedirs(output_away_path.parent, exist_ok=True)

    # Save output files to the FINAL desired location
    if not home_df.empty:
        home_df.to_csv(output_home_path, index=False)
        logger.info(f"✅ Wrote {len(home_df)} rows to {output_home_path}")
    else:
        logger.warning(f"⚠️ No home pitcher data to write. {output_home_path} not created or updated.")

    if not away_df.empty:
        away_df.to_csv(output_away_path, index=False)
        logger.info(f"✅ Wrote {len(away_df)} rows to {output_away_path}")
    else:
        logger.warning(f"⚠️ No away pitcher data to write. {output_away_path} not created or updated.")

    # --- Validation and Reporting ---
    raw_games_df = pd.read_csv(games_input_path) # Read from the temporary input
    expected = len(raw_games_df) * 2
    actual = len(home_df) + len(away_df)
    if actual != expected:
        logger.warning(f"⚠️ Mismatch: Expected {expected} total pitchers from {games_input_path}, but got {actual}")
    else:
        logger.info(f"Total pitchers generated ({actual}) matches expected count ({expected}).")

    if home_missing_pitchers:
        logger.warning(f"\n=== MISSING HOME PITCHERS ({len(set(home_missing_pitchers))} unique) ===")
        for name in sorted(set(home_missing_pitchers)):
            logger.warning(f"  - {name}")

    if away_missing_pitchers:
        logger.warning(f"\n=== MISSING AWAY PITCHERS ({len(set(away_missing_pitchers))} unique) ===")
        for name in sorted(set(away_missing_pitchers)):
            logger.warning(f"  - {name}")

    all_unmatched_teams = set(home_unmatched_teams + away_unmatched_teams)
    if all_unmatched_teams:
        logger.warning(f"\n⚠️ Unmatched team codes from {games_input_path.name} (after initial map attempt): {len(all_unmatched_teams)}")
        logger.warning("Top 5 unmatched teams:")
        for team in sorted(list(all_unmatched_teams))[:5]:
            logger.warning(f"  - {team}")

    logger.info("Pitcher data processing and standardization completed.")


if __name__ == "__main__":
    # Expect 4 command-line arguments:
    # 1. Path to pitchers_normalized_cleaned.csv (temp copy)
    # 2. Path to todaysgames_normalized.csv (temp copy)
    # 3. Path for output pitchers_home.csv (final destination)
    # 4. Path for output pitchers_away.csv (final destination)
    if len(sys.argv) != 5:
        logger.critical("Usage: python normalize_pitcher_home_away.py <pitchers_input_path> <games_input_path> <output_home_path> <output_away_path>")
        sys.exit(1)

    pitchers_input = Path(sys.argv[1])
    games_input = Path(sys.argv[2])
    output_home = Path(sys.argv[3])
    output_away = Path(sys.argv[4])

    try:
        process_pitcher_data(pitchers_input, games_input, output_home, output_away)
    except Exception as e:
        logger.critical(f"An unhandled error occurred during script execution: {e}", exc_info=True)
        sys.exit(1)

