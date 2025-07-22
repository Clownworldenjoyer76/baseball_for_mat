import pandas as pd
import subprocess
from pathlib import Path
from datetime import datetime

INPUT_FILE = "data/raw/todaysgames.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MAP_FILE = "data/Data/team_abv_map.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
SUMMARY_FILE = "summaries/A_Run_All/normalize_todays_games.txt"

def normalize_todays_games():
    try:
        # Load data
        games = pd.read_csv(INPUT_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
        pitchers = pd.read_csv(PITCHERS_FILE)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load input files: {e}")

    # Normalize teams
    team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
    games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

    # Normalize pitcher names
    def normalize_name(name):
        name = name.strip().replace(".", "")
        parts = name.split()
        return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) >= 2 else name

    games['pitcher_home'] = games['pitcher_home'].astype(str).apply(normalize_name)
    games['pitcher_away'] = games['pitcher_away'].astype(str).apply(normalize_name)

    # Validation
    valid_pitchers = set(pitchers['last_name, first_name'])
    missing_home = games[~games['pitcher_home'].isin(valid_pitchers) & (games['pitcher_home'] != 'Undecided')]
    missing_away = games[~games['pitcher_away'].isin(valid_pitchers) & (games['pitcher_away'] != 'Undecided')]

    if not missing_home.empty or not missing_away.empty:
        missing = pd.concat([missing_home, missing_away])
        raise ValueError(f"❌ Unrecognized pitcher(s) found:\n{missing[['home_team', 'away_team', 'pitcher_home', 'pitcher_away']]}")

    # Write output
    games.to_csv(OUTPUT_FILE, index=False)

    # Log summary
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}\n🕒 Timestamp: {timestamp}\n"
    Path(SUMMARY_FILE).write_text(summary)
    print(summary)

    # Git commit + push
    try:
        subprocess.run(["git", "add", OUTPUT_FILE, SUMMARY_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "🔄 Update todaysgames_normalized.csv after name fix"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"⚠️ Git commit/push failed: {e}")

if __name__ == "__main__":
    normalize_todays_games()
