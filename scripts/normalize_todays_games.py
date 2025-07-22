import pandas as pd
from pathlib import Path
import subprocess
import time
import os

# File paths
INPUT_FILE = "data/raw/todaysgames.csv"
TEAM_MAP_FILE = "data/Data/team_abv_map.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"

def normalize_name(name):
    if not isinstance(name, str):
        return name
    parts = name.strip().replace(".", "").split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name.strip()

def main():
    # Load files
    games = pd.read_csv(INPUT_FILE)
    team_map = pd.read_csv(TEAM_MAP_FILE)
    pitchers = pd.read_csv(PITCHERS_FILE)

    # Normalize team names
    team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
    games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

    # Normalize pitcher names
    games['pitcher_home'] = games['pitcher_home'].apply(normalize_name)
    games['pitcher_away'] = games['pitcher_away'].apply(normalize_name)

    valid_pitchers = set(pitchers['last_name, first_name'])
    games = games[
        (games['pitcher_home'].isin(valid_pitchers) | (games['pitcher_home'] == 'Undecided')) &
        (games['pitcher_away'].isin(valid_pitchers) | (games['pitcher_away'] == 'Undecided'))
    ]

    # Save file
    games.to_csv(OUTPUT_FILE, index=False)
    Path(OUTPUT_FILE).touch()  # Force filesystem update
    print(f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}")

    # Git force commit
    try:
        subprocess.run(["git", "config", "--global", "user.name", "Auto Commit Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "add", OUTPUT_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "🔄 Force update to todaysgames_normalized.csv"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed: {e}")

if __name__ == "__main__":
    main()
