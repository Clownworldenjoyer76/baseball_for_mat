import pandas as pd
import subprocess
from datetime import datetime

# File paths
GAMES_FILE = "data/raw/todaysgames.csv"
TEAM_MAP_FILE = "data/Data/team_abv_map.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"

SUFFIXES = {"Jr", "Sr", "II", "III", "IV", "V"}

def normalize_name(name):
    parts = name.strip().split()
    if len(parts) >= 2:
        # Handle suffixes as part of last name
        if parts[-1].strip(".") in SUFFIXES:
            last = f"{parts[-2]} {parts[-1]}"
            first = " ".join(parts[:-2])
        else:
            last = parts[-1]
            first = " ".join(parts[:-1])
        return f"{last}, {first}"
    return name.strip()

def normalize_todays_games():
    games = pd.read_csv(GAMES_FILE)
    team_map = pd.read_csv(TEAM_MAP_FILE)
    pitchers = pd.read_csv(PITCHERS_FILE)

    # Normalize team names
    team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
    games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

    # Normalize pitcher names with suffix support
    games['pitcher_home'] = games['pitcher_home'].astype(str).apply(normalize_name)
    games['pitcher_away'] = games['pitcher_away'].astype(str).apply(normalize_name)

    valid_pitchers = set(pitchers['last_name, first_name'])
    mask = (
        games['pitcher_home'].isin(valid_pitchers) |
        (games['pitcher_home'].str.lower() == 'undecided')
    ) & (
        games['pitcher_away'].isin(valid_pitchers) |
        (games['pitcher_away'].str.lower() == 'undecided')
    )

    missing = games[~mask]
    if not missing.empty:
        raise ValueError(f"❌ Unrecognized pitcher(s) found:\n{missing[['home_team', 'away_team', 'pitcher_home', 'pitcher_away']]}")

    games.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ normalize_todays
