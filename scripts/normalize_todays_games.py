import pandas as pd
import subprocess
from datetime import datetime
from pathlib import Path

# File paths
INPUT_FILE = "data/raw/todaysgames.csv"
TEAM_MAP_FILE = "data/Data/team_abv_map.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"
SUMMARY_FILE = "summaries/A_Run_All/normalize_todays_games.txt"
LOG_FILE = "summaries/A_Run_All/normalize_todays_games.log"
ERROR_LOG = "summaries/A_Run_All/errors.txt"

def normalize_todays_games():
    try:
        # Load files
        games = pd.read_csv(INPUT_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
        pitchers = pd.read_csv(PITCHERS_FILE)

        # Normalize team names
        team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
        games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
        games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

        # Normalize pitcher names
        def normalize_name(name):
            name = name.replace('.', '').strip()
            parts = name.split()
            if len(parts) >= 2:
                return f"{parts[-1]}, {' '.join(parts[:-1])}"
            return name

        games['pitcher_home'] = games['pitcher_home'].astype(str).apply(normalize_name)
        games['pitcher_away'] = games['pitcher_away'].astype(str).apply(normalize_name)

        # Allow pitchers to be in valid list OR equal to 'Undecided'
        valid_pitchers = set(pitchers['last_name, first_name'])
        games = games[
            (games['pitcher_home'].isin(valid_pitchers) | (games['pitcher_home'] == 'Undecided')) &
            (games['pitcher_away'].isin(valid_pitchers) | (games['pitcher_away'] == 'Undecided'))
        ]

        # Save file with corrected line terminator
        games.to_csv(OUTPUT_FILE, index=False, lineterminator='\n')

        summary = f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}"
        print(summary)
        Path(SUMMARY_FILE).write_text(summary)

        log_content = games.to_string(index=False)
        Path(LOG_FILE).write_text(log_content)

        # Force commit
        subprocess.run(["git", "add", OUTPUT_FILE, SUMMARY_FILE, LOG_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "🔄 Update todaysgames_normalized.csv after name fix"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")

    except Exception as e:
        error_msg = f"❌ normalize_todays_games ERROR:\n{str(e)}"
        print(error_msg)
        Path(ERROR_LOG).write_text(error_msg)

if __name__ == "__main__":
    normalize_todays_games()
