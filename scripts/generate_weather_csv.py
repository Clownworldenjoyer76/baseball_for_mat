import pandas as pd
from pathlib import Path
import subprocess
from datetime import datetime

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def generate_weather_csv():
    try:
        games_df = pd.read_csv(GAMES_FILE)
        stadium_df = pd.read_csv(STADIUM_FILE)
        team_map_df = pd.read_csv(TEAM_MAP_FILE)
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return
    except Exception as e:
        print(f"❌ Error reading input files: {e}")
        return

    # Normalize input for merging
    games_df['home_team'] = games_df['home_team'].str.strip().str.upper()
    games_df['away_team'] = games_df['away_team'].str.strip().str.upper()
    stadium_df['home_team'] = stadium_df['home_team'].str.strip().str.upper()
    team_map_df['uppercase'] = team_map_df['team_name'].str.strip().str.upper()
    team_map_df = team_map_df.drop_duplicates(subset='uppercase')

    # Drop 'game_time' to prevent suffixes
    games_df = games_df.drop(columns=['game_time'], errors='ignore')

    # Merge game and stadium data
    merged = pd.merge(games_df, stadium_df, on='home_team', how='left')
    if merged.empty:
        print("❌ Merge failed: No matching rows.")
        return

    # Fix home_team to official casing
    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']],
        left_on='home_team',
        right_on='uppercase',
        how='left'
    )
    merged.drop(columns=['home_team', 'uppercase'], inplace=True)
    merged.rename(columns={'team_name': 'home_team'}, inplace=True)

    # Fix away_team to official casing
    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']],
        left_on='away_team',
        right_on='uppercase',
        how='left'
    )
    merged.drop(columns=['away_team', 'uppercase'], inplace=True)
    merged.rename(columns={'team_name': 'away_team'}, inplace=True)

    # Write final CSV
    merged.to_csv(OUTPUT_FILE, index=False)

    # Write summary
    Path(SUMMARY_FILE).parent.mkdir(parents=True, exist_ok=True)
    summary = (
        f"✅ Weather input file generated\n"
        f"🔢 Rows: {len(merged)}\n"
        f"📁 Output: {OUTPUT_FILE}\n"
        f"📄 Games file: {GAMES_FILE}\n"
        f"🏟️ Stadium file: {STADIUM_FILE}"
    )
    print(summary)
    Path(SUMMARY_FILE).write_text(summary)

    # Force Git commit
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subprocess.run(["git", "add", OUTPUT_FILE, SUMMARY_FILE], check=True)
        subprocess.run(["git", "commit", "-m", f"🔁 Update weather_input.csv at {timestamp}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed: {e}")

if __name__ == "__main__":
    generate_weather_csv()
