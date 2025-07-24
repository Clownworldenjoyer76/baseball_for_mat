import pandas as pd
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import os
import logging
import traceback

# Setup logging
log_dir = Path("summaries")
log_dir.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = log_dir / f"merge_game_pitcher_data_{timestamp}.log"

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler(sys.stdout) # Use sys.stdout to ensure it prints
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)
logging.getLogger().setLevel(logging.INFO) # Ensure root logger level is INFO


def standardize_name(full_name):
    if pd.isna(full_name) or str(full_name).strip().lower() == "undecided":
        return full_name
    parts = str(full_name).strip().split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return str(full_name).title()

def verify_columns(df, required, label):
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing column(s) {missing_cols} in {label} file. Available columns: {df.columns.tolist()}")

def get_pitcher_woba(df, name_col):
    required = [name_col, "adj_woba_combined"]
    verify_columns(df, required, "pitcher")
    return df[required].drop_duplicates(subset=[name_col])

def safe_read_csv(filepath, label):
    if not Path(filepath).is_file():
        raise FileNotFoundError(f"❌ {filepath} ({label}) does not exist")
    df = pd.read_csv(filepath)
    logging.info(f"✅ Loaded {label}: {len(df)} rows")
    return df

def main():
    # File paths
    BATTERS_HOME_FILE = "data/adjusted/batters_home_weather_park.csv"
    BATTERS_AWAY_FILE = "data/adjusted/batters_away_weather_park.csv"
    PITCHERS_HOME_FILE = "data/adjusted/pitchers_home_weather_park.csv"
    PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away_weather_park.csv"
    GAMES_FILE = "data/raw/todaysgames_normalized.csv"

    OUTPUT_HOME = "data/processed/batters_home_with_pitcher.csv"
    OUTPUT_AWAY = "data/processed/batters_away_with_pitcher.csv"

    logging.info("Starting merge_game_pitcher_data script.")

    try:
        bh = safe_read_csv(BATTERS_HOME_FILE, "batters_home")
        ba = safe_read_csv(BATTERS_AWAY_FILE, "batters_away")
        ph = safe_read_csv(PITCHERS_HOME_FILE, "pitchers_home")
        pa = safe_read_csv(PITCHERS_AWAY_FILE, "pitchers_away")
        games = safe_read_csv(GAMES_FILE, "games")
    except FileNotFoundError as e:
        logging.error(f"❌ Required input file not found: {e}")
        raise # Re-raise to fail the script if files are missing

    # Validation
    verify_columns(bh, ["team", "last_name, first_name"], "batters_home")
    verify_columns(ba, ["team", "last_name, first_name"], "batters_away")
    verify_columns(games, ["home_team", "away_team", "pitcher_home", "pitcher_away"], "games")

    # Standardize names for merging
    for df in [bh, ba, ph, pa]:
        if "last_name, first_name" in df.columns:
            df["last_name, first_name"] = df["last_name, first_name"].apply(standardize_name)

    # Convert team columns to uppercase for consistent merging with games_df
    if "team" in bh.columns:
        bh["team"] = bh["team"].astype(str).str.strip().str.upper()
    if "team" in ba.columns:
        ba["team"] = ba["team"].astype(str).str.strip().str.upper()
    
    # Ensure games_df team names are also consistently upper for the merge keys
    if "home_team" in games.columns:
        games["home_team"] = games["home_team"].astype(str).str.strip().str.upper()
    if "away_team" in games.columns:
        games["away_team"] = games["away_team"].astype(str).str.strip().str.upper()

    # --- DEBUGGING TEAM NAMES ---
    logging.info(f"DEBUG: Unique 'team' values in batters_home (bh['team']):\n{bh['team'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: Unique 'home_team' values in games (games['home_team']):\n{games['home_team'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: Unique 'team' values in batters_away (ba['team']):\n{ba['team'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: Unique 'away_team' values in games (games['away_team']):\n{games['away_team'].value_counts(dropna=False).to_string()}")
    # --- END DEBUGGING TEAM NAMES ---

    logging.info("DEBUG: Merging games data to batter dataframes to get pitcher names.")
    # Merge games_df into batter dataframes to get the specific pitcher for that game
    bh = bh.merge(games[["home_team", "pitcher_home", "game_time"]], how="left", left_on="team", right_on="home_team")
    ba = ba.merge(games[["away_team", "pitcher_away", "game_time"]], how="left", left_on="team", right_on="away_team")

    # Debugging: Verify 'pitcher_home'/'pitcher_away' are now in the dataframes and check for NaNs
    if 'pitcher_home' not in bh.columns:
        logging.error("❌ 'pitcher_home' column not found in batters_home after first merge. This is critical. Check team name consistency.")
        raise KeyError("'pitcher_home' column not found after initial merge")
    if 'pitcher_away' not in ba.columns:
        logging.error("❌ 'pitcher_away' column not found in batters_away after first merge. This is critical. Check team name consistency.")
        raise KeyError("'pitcher_away' column not found after initial merge")

    logging.info(f"DEBUG: bh['pitcher_home'] value counts after 1st merge (should have pitcher names):\n{bh['pitcher_home'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: ba['pitcher_away'] value counts after 1st merge (should have pitcher names):\n{ba['pitcher_away'].value_counts(dropna=False).to_string()}")

    logging.info("DEBUG: Merging pitcher wOBA data into batter dataframes.")
    # Then merge pitcher wOBA data
    # Ensure to use a unique suffix if name columns might conflict
    bh = bh.merge(get_pitcher_woba(ph, "last_name, first_name"), how="left",
                  left_on="pitcher_home", right_on="last_name, first_name", suffixes=("", "_pitcher_woba"))
    ba = ba.merge(get_pitcher_woba(pa, "last_name, first_name"), how="left",
                  left_on="pitcher_away", right_on="last_name, first_name", suffixes=("", "_pitcher_woba"))

    logging.info(f"✅ HOME batters final rows: {len(bh)}")
    logging.info(f"✅ AWAY batters final rows: {len(ba)}")

    # Ensure output directory exists
    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_AWAY).parent.mkdir(parents=True, exist_ok=True)

    bh.to_csv(OUTPUT_HOME, index=False)
    ba.to_csv(OUTPUT_AWAY, index=False)
    logging.info(f"📁 Files saved: {OUTPUT_HOME}, {OUTPUT_AWAY}")

    # --- Git Operations ---
    try:
        logging.info("DEBUG: Starting Git operations for merge_game_pitcher_data.")
        logging.info(f"DEBUG: Attempting to git add all modified files (git add .).")
        subprocess.run(["git", "add", "."], check=True, capture_output=True, text=True)
        logging.info("DEBUG: Git add . successful. All changes staged.")

        status_output = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if not status_output.strip():
            logging.info("✅ No changes to commit. Workflow files are up to date.")
        else:
            logging.info("DEBUG: Changes detected, attempting to commit.")
            subprocess.run(["git", "commit", "-m", "📝 Update merged game and pitcher data and other workflow files"], check=True, capture_output=True, text=True)
            logging.info("DEBUG: Git commit successful.")
            subprocess.run(["git", "push"], check=True, capture_output=True, text=True)
            logging.info("✅ Git commit and push complete for merged game/pitcher data.")

    except subprocess.CalledProcessError as e:
        logging.error(f"⚠️ Git commit/push failed for merged game/pitcher data:")
        logging.error(f"  Command: {e.cmd}")
        logging.error(f"  Return Code: {e.returncode}")
        logging.error(f"  STDOUT: {e.stdout}")
        logging.error(f"  STDERR: {e.stderr}")
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred during Git operations for merged game/pitcher data: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Script failed: {e}")
        # traceback.print_exc() # Removed this as per previous decision
    finally:
        logging.info("📝 Final debug log completed")
