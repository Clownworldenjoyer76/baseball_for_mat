import pandas as pd
from pathlib import Path
import subprocess

INPUT_FILE = "data/raw/todaysgames.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MAP_FILE = "data/Data/team_abv_map.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
SUMMARY_FILE = "summaries/A_Run_All/normalize_todays_games.txt"

def normalize_todays_games():
    try:
        games = pd.read_csv(INPUT_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
        pitchers = pd.read_csv(PITCHERS_FILE)

        # Normalize team names
        team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
        games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
        games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

        # Normalize pitcher names
        def normalize_name(name):
            if pd.isna(name): return "Undecided"
            name = name.replace(".", "").strip()
            parts = name.split()
            return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) >= 2 else name

        games['pitcher_home'] = games['pitcher_home'].apply(normalize_name)
        games['pitcher_away'] = games['pitcher_away'].apply(normalize_name)

        # Filter invalid pitchers
        valid_pitchers = set(pitchers['last_name, first_name'])
        games = games[
            (games['pitcher_home'].isin(valid_pitchers) | (games['pitcher_home'] == 'Undecided')) &
            (games['pitcher_away'].isin(valid_pitchers) | (games['pitcher_away'] == 'Undecided'))
        ]

        # Write output
        games.to_csv(OUTPUT_FILE, index=False)

        summary = f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}"
        print(summary)
        Path(SUMMARY_FILE).write_text(summary)

        # Git push
        subprocess.run(["git", "add", OUTPUT_FILE, SUMMARY_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "🔄 Update todaysgames_normalized.csv after name fix"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")

    except Exception as e:
        error_msg = f"❌ normalize_todays_games ERROR:\n{e}"
        print(error_msg)
        Path(SUMMARY_FILE).write_text(error_msg)

if __name__ == "__main__":
    normalize_todays_games()
