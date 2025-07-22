import pandas as pd
import subprocess
import os
from pathlib import Path
from datetime import datetime

# File paths
GAMES_FILE = 'data/raw/todaysgames.csv'
TEAM_MAP_FILE = 'data/Data/team_abv_map.csv'
PITCHERS_FILE = 'data/cleaned/pitchers_normalized_cleaned.csv'
OUTPUT_FILE = 'data/raw/todaysgames_normalized.csv'
SUMMARY_FILE = 'summaries/A_Run_All/normalize_todays_games.txt'

def normalize_todays_games():
    try:
        games = pd.read_csv(GAMES_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
        pitchers = pd.read_csv(PITCHERS_FILE)
    except Exception as e:
        print(f"❌ File load error: {e}")
        return

    team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
    games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

    def normalize_name(name):
        name = str(name).replace(".", "").strip()
        parts = name.split()
        return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) >= 2 else name

    games['pitcher_home'] = games['pitcher_home'].astype(str).apply(normalize_name)
    games['pitcher_away'] = games['pitcher_away'].astype(str).apply(normalize_name)

    valid_pitchers = set(pitchers['last_name, first_name'])
    games = games[
        (games['pitcher_home'].isin(valid_pitchers) | (games['pitcher_home'] == 'Undecided')) &
        (games['pitcher_away'].isin(valid_pitchers) | (games['pitcher_away'] == 'Undecided'))
    ]

    try:
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)
        games.to_csv(OUTPUT_FILE, index=False)
    except Exception as e:
        print(f"❌ Write error: {e}")
        return

    summary = (
        f"✅ normalize_todays_games.py completed: {len(games)} rows written

    
