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
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

def standardize_name(full_name):
    if pd.isna(full_name) or full_name.strip().lower() == "undecided":
        return full_name
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return full_name.title()

def verify_columns(df, required, label):
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in {label} file")

def get_pitcher_woba(df, name_col):
    required = [name_col, "adj_woba_combined"]
    verify_columns(df, required, "pitcher")
    return df[required].drop_duplicates(subset=[name_col])

def safe_read_csv(filepath, label):
    if not Path(filepath).is_file():
        raise FileNotFoundError(f"{filepath} ({label}) does not exist")
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

    bh = safe_read_csv(BATTERS_HOME_FILE, "batters_home")
    ba = safe_read_csv(BATTERS_AWAY_FILE, "batters_away")
    ph = safe_read_csv(PITCHERS_HOME_FILE, "pitchers_home")
    pa = safe_read_csv(PITCHERS_AWAY_FILE, "pitchers_away")
    games = safe_read_csv(GAMES_FILE, "games")

    # Validation
    verify_columns(bh, ["team", "last_name, first_name"], "batters_home")
    verify_columns(ba, ["team", "last_name, first_name"], "batters_away")
    verify_columns(games, ["home_team", "away_team", "pitcher_home", "pitcher_away"], "games")

    # Standardize names
    for df in [bh, ba, ph, pa]:
        df["last_name, first_name"] = df["last_name, first_name"].apply(standardize_name)
    games["pitcher_home"] = games["pitcher_home"].fillna("").apply(standardize_name)
    games["pitcher_away"] = games["pitcher_away"].fillna("").apply(standardize_name)

    # Merge pitcher names
    bh = bh.merge(games[["home_team", "pitcher_home"]], how="left", left_on="team", right_on="home_team")
    ba = ba.merge(games[["away_team", "pitcher_away"]], how="left", left_on="team", right_on="away_team")

    bh = bh.merge(get_pitcher_woba(ph, "last_name, first_name"), how="left",
                  left_on="pitcher_home", right_on="last_name, first_name", suffixes=("", "_pitcher"))
    ba = ba.merge(get_pitcher_woba(pa, "last_name, first_name"), how="left",
                  left_on="pitcher_away", right_on="last_name, first_name", suffixes=("", "_pitcher"))

    logging.info(f"✅ HOME batters final rows: {len(bh)}")
    logging.info(f"✅ AWAY batters final rows: {len(ba)}")

    bh.to_csv(OUTPUT_HOME, index=False)
    ba.to_csv(OUTPUT_AWAY, index=False)
    logging.info(f"📁 Files saved: {OUTPUT_HOME}, {OUTPUT_AWAY}")

    subprocess.run(["git", "add", OUTPUT_HOME, OUTPUT_AWAY], check=True)
    subprocess.run(["git", "commit", "-m", "Add merged batter and pitcher matchup stats"], check=True)
    subprocess.run(["git", "push"], check=True)
    logging.info("🚀 Git changes pushed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Script failed: {e}")
        traceback.print_exc()
    finally:
        logging.info("📝 Final debug log completed")
