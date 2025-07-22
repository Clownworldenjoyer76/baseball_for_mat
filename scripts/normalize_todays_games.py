import pandas as pd
import os
import subprocess
from datetime import datetime

# File paths
GAMES_FILE = "data/raw/todaysgames.csv"
TEAM_MAP_FILE = "data/Data/team_abv_map.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"
SUMMARY_LOG = "summaries/A_Run_All/normalize_todays_games.log"

# Load files
games = pd.read_csv(GAMES_FILE)
team_map = pd.read_csv(TEAM_MAP_FILE)
pitchers = pd.read_csv(PITCHERS_FILE)

# Normalize team names
team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

# Normalize pitcher names (suffix-aware)
def normalize_name(name):
    parts = name.strip().split()
    if len(parts) >= 2 and parts[-1] in ["Jr", "Sr", "II", "III", "IV"]:
        last_name = " ".join(parts[-2:])
        first_name = " ".join(parts[:-2])
        return f"{last_name}, {first_name}"
    elif len(parts) >= 2:
        last_name = parts[-1]
        first_name = " ".join(parts[:-1])
        return f"{last_name}, {first_name}"
    return name.strip()

games['pitcher_home'] = games['pitcher_home'].apply(normalize_name)
games['pitcher_away'] = games['pitcher_away'].apply(normalize_name)

# Validate pitchers
valid_pitchers = set(pitchers['last_name, first_name'])
missing = games[
    ~games['pitcher_home'].isin(valid_pitchers) |
    ~games['pitcher_away'].isin(valid_pitchers)
]

if not missing.empty:
    raise ValueError(f"❌ Unrecognized pitcher(s) found:\n{missing[['home_team', 'away_team', 'pitcher_home', 'pitcher_away']]}")

# Save normalized file
games.to_csv(OUTPUT_FILE, index=False)

# Write summary
with open(SUMMARY_LOG, "w") as f:
    f.write(f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}\n")

# Git commit and push
try:
    subprocess.run(["git", "add", OUTPUT_FILE], check=True)
    subprocess.run(["git", "commit", "-m", "🔄 Update todaysgames_normalized.csv after name fix"], check=True)
    subprocess.run(["git", "push"], check=True)
except subprocess.CalledProcessError as e:
    with open(SUMMARY_LOG, "a") as f:
        f.write(f"⚠️ Git commit/push failed: {e}\n")

print(f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}")
