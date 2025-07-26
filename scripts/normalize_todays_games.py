import pandas as pd
import re
from unidecode import unidecode
from datetime import datetime
from pathlib import Path
import sys

INPUT_FILE = "data/raw/todaysgames.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"

def is_valid_time(t):
    try:
        datetime.strptime(t.strip(), "%I:%M %p")
        return True
    except Exception:
        return False

def normalize_todays_games():
    print("📥 Loading input files...")
    try:
        games = pd.read_csv(INPUT_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
    except Exception as e:
        print(f"❌ Error loading input files: {e}")
        sys.exit(1)

    print("🔁 Mapping team abbreviations to full names...")
    team_map["team_code"] = team_map["team_code"].astype(str).str.strip().str.upper()
    team_map["team_name"] = team_map["team_name"].astype(str).str.strip()
    code_to_name = dict(zip(team_map["team_code"], team_map["team_name"]))

    for col in ["home_team", "away_team"]:
        original = games[col].astype(str).str.strip().str.upper()
        games[col] = original.map(code_to_name)
        unmapped = original[games[col].isna()].unique()
        if len(unmapped) > 0:
            print(f"⚠️ Unmapped {col} codes: {list(unmapped)}")
        games[col] = games[col].fillna(original)

    print("⏱ Validating game times...")
    invalid_times = games[~games["game_time"].apply(is_valid_time)]
    if not invalid_times.empty:
        print("❌ Invalid game_time values:")
        print(invalid_times[["home_team", "away_team", "game_time"]])
        sys.exit(1)

    print("🔁 Checking for duplicate matchups...")
    dupes = games.duplicated(subset=["home_team", "away_team"], keep=False)
    if dupes.any():
        print("❌ Duplicate matchups found:")
        print(games[dupes])
        sys.exit(1)

    games.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ normalize_todays_games completed: {OUTPUT_FILE}")

if __name__ == "__main__":
    normalize_todays_games()
