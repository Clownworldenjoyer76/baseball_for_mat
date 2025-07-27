import pandas as pd
import logging
from pathlib import Path
import sys
import shutil
import os

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")

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

def process_pitcher_data(pitchers_input_path: Path, games_input_path: Path,
                         output_home_path: Path, output_away_path: Path):
    logger.info("Starting pitcher data processing and standardization.")
    team_map = load_team_map()
    logger.info(f"Loaded team map with {len(team_map)} entries.")

    if not pitchers_input_path.exists():
        logger.critical(f"❌ Missing pitcher input file: {pitchers_input_path}")
        raise FileNotFoundError(f"{pitchers_input_path} does not exist.")
    pitchers_df = pd.read_csv(pitchers_input_path)
    pitchers_df["name"] = pitchers_df["name"].astype(str).str.strip()
    pitchers_df = pitchers_df.drop_duplicates(subset=["name", "team"])
    logger.info(f"Loaded {len(pitchers_df)} unique pitchers from {pitchers_input_path}.")

    if not games_input_path.exists():
        logger.critical(f"❌ Missing games input file: {games_input_path}")
        raise FileNotFoundError(f"{games_input_path} does not exist.")
    full_games_df = pd.read_csv(games_input_path)[["pitcher_home", "pitcher_away", "home_team", "away_team"]]
    logger.info(f"Loaded {len(full_games_df)} games from {games_input_path}.")
    full_games_df['home_team'] = full_games_df['home_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['home_team'])
    full_games_df['away_team'] = full_games_df['away_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['away_team'])

    home_tagged, home_missing_pitchers, home_unmatched_teams = [], [], []
    for _, row in full_games_df.iterrows():
        pitcher_name = row['pitcher_home']
        game_home_team = row['home_team']
        game_away_team = row['away_team']
        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()
        if matched.empty:
            home_missing_pitchers.append(pitcher_name)
            home_unmatched_teams.append(game_home_team)
        else:
            matched["team"] = game_home_team
            matched["home_away"] = "home"
            matched["game_home_team"] = game_home_team
            matched["game_away_team"] = game_away_team
            home_tagged.append(matched)

    home_df = pd.concat(home_tagged, ignore_index=True) if home_tagged else pd.DataFrame()
    if not home_df.empty:
        home_df.drop(columns=[col for col in home_df.columns if col.endswith(".1")], errors='ignore', inplace=True)
        home_df.sort_values(by=["team", "name"], inplace=True)
        home_df.drop_duplicates(inplace=True)
        home_df["team"] = home_df["team"].astype(str).str.strip().map(team_map).fillna(home_df["team"])
        logger.info(f"Generated {len(home_df)} home pitcher records.")

    away_tagged, away_missing_pitchers, away_unmatched_teams = [], [], []
    for _, row in full_games_df.iterrows():
        pitcher_name = row['pitcher_away']
        game_home_team = row['home_team']
        game_away_team = row['away_team']
        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()
        if matched.empty:
            away_missing_pitchers.append(pitcher_name)
            away_unmatched_teams.append(game_away_team)
        else:
            matched["team"] = game_away_team
            matched["home_away"] = "away"
            matched["game_home_team"] = game_home_team
            matched["game_away_team"] = game_away_team
            away_tagged.append(matched)

    away_df = pd.concat(away_tagged, ignore_index=True) if away_tagged else pd.DataFrame()
    if not away_df.empty:
        away_df.drop(columns=[col for col in away_df.columns if col.endswith(".1")], errors='ignore', inplace=True)
        away_df.sort_values(by=["team", "name"], inplace=True)
        away_df.drop_duplicates(inplace=True)
        away_df["team"] = away_df["team"].astype(str).str.strip().map(team_map).fillna(away_df["team"])
        logger.info(f"Generated {len(away_df)} away pitcher records.")

    os.makedirs(output_home_path.parent, exist_ok=True)
    os.makedirs(output_away_path.parent, exist_ok=True)
    if not home_df.empty:
        home_df.to_csv(output_home_path, index=False)
        logger.info(f"✅ Wrote {len(home_df)} rows to {output_home_path}")
    if not away_df.empty:
        away_df.to_csv(output_away_path, index=False)
        logger.info(f"✅ Wrote {len(away_df)} rows to {output_away_path}")

    raw_games_df = pd.read_csv(games_input_path)
    expected = len(raw_games_df) * 2
    actual = len(home_df) + len(away_df)
    if actual != expected:
        logger.warning(f"⚠️ Mismatch: Expected {expected} total pitchers from {games_input_path}, but got {actual}")
    else:
        logger.info(f"Total pitchers generated ({actual}) matches expected count ({expected}).")

    if home_missing_pitchers:
        logger.warning(f"=== MISSING HOME PITCHERS ({len(set(home_missing_pitchers))} unique) ===")
        for name in sorted(set(home_missing_pitchers)):
            logger.warning(f"  - {name}")
    if away_missing_pitchers:
        logger.warning(f"=== MISSING AWAY PITCHERS ({len(set(away_missing_pitchers))} unique) ===")
        for name in sorted(set(away_missing_pitchers)):
            logger.warning(f"  - {name}")
    all_unmatched_teams = set(home_unmatched_teams + away_unmatched_teams)
    if all_unmatched_teams:
        logger.warning(f"⚠️ Unmatched team codes: {len(all_unmatched_teams)}")
        for team in sorted(list(all_unmatched_teams))[:5]:
            logger.warning(f"  - {team}")

    logger.info("Pitcher data processing and standardization completed.")

if __name__ == "__main__":
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
