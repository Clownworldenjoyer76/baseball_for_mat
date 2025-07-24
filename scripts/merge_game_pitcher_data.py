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
    verify_columns(games, ["home_team", "away_team", "pitcher_home", "pitcher_away", "game_time"], "games") # Ensure game_time is also checked

    # Drop existing pitcher/game columns from batter dfs if they exist (from previous incomplete merges)
    for col in ["home_team", "away_team", "pitcher_home", "pitcher_away", "game_time"]:
        if col in bh.columns:
            bh = bh.drop(columns=[col])
        if col in ba.columns:
            ba = ba.drop(columns=[col])
    # Also drop any suffixed columns from previous attempts if they somehow persist
    for col in ["home_team_x", "away_team_x", "pitcher_home_x", "pitcher_away_x", "game_time_x", "home_team_y", "away_team_y", "pitcher_home_y", "pitcher_away_y", "game_time_y"]:
        if col in bh.columns:
            bh = bh.drop(columns=[col])
        if col in ba.columns:
            ba = ba.drop(columns=[col])


    # Standardize player names for future merges with pitcher data
    # Note: `last_name, first_name` is used here, assuming it's the consistent column for player names
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

    # --- DEBUGGING TEAM NAMES AND DATAFRAME HEADS BEFORE MERGE ---
    logging.info(f"DEBUG: Unique 'team' values in batters_home (bh['team']):\n{bh['team'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: Unique 'home_team' values in games (games['home_team']):\n{games['home_team'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: bh.head() before 1st merge:\n{bh.head().to_string()}")
    logging.info(f"DEBUG: games[['home_team', 'pitcher_home', 'game_time']].head() before 1st merge:\n{games[['home_team', 'pitcher_home', 'game_time']].head().to_string()}")
    # --- END DEBUGGING TEAM NAMES AND DATAFRAME HEADS BEFORE MERGE ---

    logging.info("DEBUG: Merging games data to batter dataframes to get pitcher names.")
    
    # Perform the merge and store it in a temporary variable to inspect
    # Suffixes are implicitly handled by pandas if column names conflict beyond merge keys
    temp_bh = bh.merge(games[["home_team", "pitcher_home", "game_time"]], how="left", left_on="team", right_on="home_team")
    
    # Rename the merged columns from games for consistency
    temp_bh = temp_bh.rename(columns={"pitcher_home": "pitcher_home_temp_x", "game_time": "game_time_temp_x",
                                      "pitcher_home_y": "pitcher_home", "game_time_y": "game_time"})
    # Drop the original columns from bh if they weren't explicitly handled by suffixes (unlikely for pitch_home but good practice)
    if "pitcher_home_temp_x" in temp_bh.columns: # This will be the original column if it existed
        temp_bh = temp_bh.drop(columns=["pitcher_home_temp_x"])
    if "game_time_temp_x" in temp_bh.columns:
        temp_bh = temp_bh.drop(columns=["game_time_temp_x"])
    
    # Log columns and a sample of pitcher_home from the temporary DataFrame
    logging.info(f"DEBUG: Columns of temp_bh after 1st merge attempt and renaming: {temp_bh.columns.tolist()}")
    logging.info(f"DEBUG: temp_bh['pitcher_home'] value counts after 1st merge attempt and renaming:\n{temp_bh.get('pitcher_home', pd.Series(dtype='object')).value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: temp_bh.head() after 1st merge attempt and renaming:\n{temp_bh.head().to_string()}") 
    
    # Assign the result back to bh only after logging
    bh = temp_bh

    # Repeat for away batters
    logging.info(f"DEBUG: Unique 'team' values in batters_away (ba['team']):\n{ba['team'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: Unique 'away_team' values in games (games['away_team']):\n{games['away_team'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: ba.head() before 1st merge:\n{ba.head().to_string()}")
    logging.info(f"DEBUG: games[['away_team', 'pitcher_away', 'game_time']].head() before 1st merge:\n{games[['away_team', 'pitcher_away', 'game_time']].head().to_string()}")

    temp_ba = ba.merge(games[["away_team", "pitcher_away", "game_time"]], how="left", left_on="team", right_on="away_team")

    # Rename the merged columns from games for consistency
    temp_ba = temp_ba.rename(columns={"pitcher_away": "pitcher_away_temp_x", "game_time": "game_time_temp_x",
                                      "pitcher_away_y": "pitcher_away", "game_time_y": "game_time"})
    if "pitcher_away_temp_x" in temp_ba.columns:
        temp_ba = temp_ba.drop(columns=["pitcher_away_temp_x"])
    if "game_time_temp_x" in temp_ba.columns:
        temp_ba = temp_ba.drop(columns=["game_time_temp_x"])

    logging.info(f"DEBUG: Columns of temp_ba after 1st merge attempt and renaming: {temp_ba.columns.tolist()}")
    logging.info(f"DEBUG: temp_ba['pitcher_away'] value counts after 1st merge attempt and renaming:\n{temp_ba.get('pitcher_away', pd.Series(dtype='object')).value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: temp_ba.head() after 1st merge attempt and renaming:\n{temp_ba.head().to_string()}")
    ba = temp_ba

    # The existing checks will now apply to bh and ba which were just assigned
    if 'pitcher_home' not in bh.columns:
        logging.error("❌ 'pitcher_home' column not found in batters_home after first merge. This is critical. Check column naming and merging logic.")
        raise KeyError("'pitcher_home' column not found after initial merge")
    if 'pitcher_away' not in ba.columns:
        logging.error("❌ 'pitcher_away' column not found in batters_away after first merge. This is critical. Check column naming and merging logic.")
        raise KeyError("'pitcher_away' column not found after initial merge")

    # Standardize pitcher names found in games DataFrame (now in bh/ba)
    if 'pitcher_home' in bh.columns:
        bh['pitcher_home'] = bh['pitcher_home'].apply(standardize_name)
    if 'pitcher_away' in ba.columns:
        ba['pitcher_away'] = ba['pitcher_away'].apply(standardize_name)
    
    logging.info(f"DEBUG: bh['pitcher_home'] value counts after pitcher name standardization:\n{bh['pitcher_home'].value_counts(dropna=False).to_string()}")
    logging.info(f"DEBUG: ba['pitcher_away'] value counts after pitcher name standardization:\n{ba['pitcher_away'].value_counts(dropna=False).to_string()}")


    logging.info("DEBUG: Merging pitcher wOBA data into batter dataframes.")
    # Then merge pitcher wOBA data
    bh = bh.merge(get_pitcher_woba(ph, "last_name, first_name"), how="left",
                  left_on="pitcher_home", right_on="last_name, first_name", suffixes=("_batter", "_pitcher_woba"))
    ba = ba.merge(get_pitcher_woba(pa, "last_name, first_name"), how="left",
                  left_on="pitcher_away", right_on="last_name, first_name", suffixes=("_batter", "_pitcher_woba"))

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
    finally:
        logging.info("📝 Final debug log completed")
