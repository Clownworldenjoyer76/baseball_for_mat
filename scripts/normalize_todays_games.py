import pandas as pd
from datetime import datetime
import sys

INPUT_FILE = "data/raw/todaysgames.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"

def is_valid_time(t):
    try:
        datetime.strptime(str(t).strip(), "%I:%M %p")
        return True
    except Exception:
        return False

def normalize_todays_games():
    print("📥 Loading input files...")
    try:
        games = pd.read_csv(INPUT_FILE, dtype=str)
        team_map = pd.read_csv(TEAM_MAP_FILE, dtype=str)
    except Exception as e:
        print(f"❌ Error loading input files: {e}")
        sys.exit(1)

    # 1) Normalize headers
    games.columns = games.columns.str.strip()
    team_map.columns = team_map.columns.str.strip()

    # Validate required columns
    for col in ["home_team", "away_team", "game_time"]:
        if col not in games.columns:
            print(f"❌ Missing required column in games: {col}")
            sys.exit(1)

    for col in ["team_code", "team_name"]:
        if col not in team_map.columns:
            print(f"❌ Missing required column in team map: {col}")
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

    print("🔁 Checking for duplicate matchups (by teams + time)…")
    # Include time so doubleheaders don’t trigger false positives
    dupe_mask = games.duplicated(subset=["home_team", "away_team", "game_time"], keep=False)
    if dupe_mask.any():
        # If exact duplicates are present, you can decide to fail here.
        exact_dupes = games[dupe_mask].sort_values(["home_team","away_team","game_time"])
        # print("❌ Exact duplicate rows found:")
        # print(exact_dupes)
        # sys.exit(1)
        pass

    games.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ normalize_todays_games completed: {OUTPUT_FILE}")

if __name__ == "__main__":
    normalize_todays_games()
