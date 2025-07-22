import pandas as pd
import subprocess
import datetime

# File paths
INPUT_FILE = 'data/raw/todaysgames.csv'
OUTPUT_FILE = 'data/raw/todaysgames_normalized.csv'
TEAM_MAP_FILE = 'data/Data/team_abv_map.csv'
PITCHERS_FILE = 'data/cleaned/pitchers_normalized_cleaned.csv'

def normalize_name(name):
    suffixes = {"Jr", "Sr", "II", "III", "IV"}
    parts = name.strip().replace(".", "").split()
    if len(parts) >= 2:
        if parts[-1] in suffixes:
            last_name = parts[-2]
            suffix = parts[-1]
            first_names = " ".join(parts[:-2])
            return f"{last_name}, {first_names} {suffix}".strip()
        else:
            last_name = parts[-1]
            first_names = " ".join(parts[:-1])
            return f"{last_name}, {first_names}".strip()
    return name.strip()

def normalize_todays_games():
    games = pd.read_csv(INPUT_FILE)
    team_map = pd.read_csv(TEAM_MAP_FILE)
    pitchers = pd.read_csv(PITCHERS_FILE)

    # Team code normalization
    team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
    games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

    # Normalize pitcher names
    games['pitcher_home'] = games['pitcher_home'].astype(str).apply(normalize_name)
    games['pitcher_away'] = games['pitcher_away'].astype(str).apply(normalize_name)

    # Validate pitchers
    valid_pitchers = set(pitchers['last_name, first_name'])
    missing = games[
        ~games['pitcher_home'].isin(valid_pitchers) &
        (games['pitcher_home'] != "Undecided")
        |
        ~games['pitcher_away'].isin(valid_pitchers) &
        (games['pitcher_away'] != "Undecided")
    ]
    if not missing.empty:
        raise ValueError(f"❌ Unrecognized pitcher(s) found:\n{missing[['home_team', 'away_team', 'pitcher_home', 'pitcher_away']]}")

    games.to_csv(OUTPUT_FILE, index=False)

    # Force git commit/push
    try:
        subprocess.run(["git", "add", OUTPUT_FILE], check=True)
        commit_msg = "🔄 Update todaysgames_normalized.csv after name fix"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed: {e}")

    print(f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}")
    print("🕒 Timestamp:", datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p %Z"))

if __name__ == "__main__":
    normalize_todays_games()
