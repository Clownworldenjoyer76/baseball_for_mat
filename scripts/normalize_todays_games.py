import pandas as pd
import subprocess
from pathlib import Path
from datetime import datetime

# File paths
GAMES_FILE = 'data/raw/todaysgames.csv'
TEAM_MAP_FILE = 'data/Data/team_abv_map.csv'
PITCHERS_FILE = 'data/cleaned/pitchers_normalized_cleaned.csv'
OUTPUT_FILE = 'data/raw/todaysgames_normalized.csv'
SUMMARY_FILE = 'summaries/A_Run_All/normalize_todays_games.txt'

def normalize_name(name):
    """Convert 'John Smith' or 'John R. Smith' to 'Smith, John' format."""
    name = str(name).strip().replace(".", "")
    parts = name.split()
    return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) >= 2 else name

def normalize_todays_games():
    print("📥 Loading data files...")
    try:
        games = pd.read_csv(GAMES_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
        pitchers = pd.read_csv(PITCHERS_FILE)
    except Exception as e:
        print(f"❌ File load error: {e}")
        return

    print("🔠 Normalizing team names...")
    team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
    games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

    print("🧼 Normalizing pitcher names...")
    games['pitcher_home'] = games['pitcher_home'].apply(normalize_name)
    games['pitcher_away'] = games['pitcher_away'].apply(normalize_name)

    print("✅ Valid pitcher names loaded:", len(pitchers))
    valid_pitchers = set(pitchers['last_name, first_name'])

    print("🔍 Filtering valid games with confirmed pitchers or 'Undecided'...")
    pre_filter_count = len(games)
    games = games[
        (games['pitcher_home'].isin(valid_pitchers) | (games['pitcher_home'] == 'Undecided')) &
        (games['pitcher_away'].isin(valid_pitchers) | (games['pitcher_away'] == 'Undecided'))
    ]
    post_filter_count = len(games)
    print(f"📉 Filtered from {pre_filter_count} → {post_filter_count} games")

    print("💾 Writing normalized games file...")
    timestamp_comment = f"# Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    games.to_csv(OUTPUT_FILE, index=False, line_terminator='\n')
    with open(OUTPUT_FILE, "a") as f:
        f.write(f"\n{timestamp_comment}\n")

    print("📝 Writing summary file...")
    summary = (
        f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}\n"
        f"🕒 Timestamp: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p %Z')}"
    )
    Path(SUMMARY_FILE).write_text(summary)

    print("📤 Committing and pushing to Git...")
    try:
        subprocess.run(["git", "add", OUTPUT_FILE, SUMMARY_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "🔄 Update todaysgames_normalized.csv after name fix"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push successful.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed: {e}")

if __name__ == "__main__":
    normalize_todays_games()
