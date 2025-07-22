import pandas as pd
from pathlib import Path
import subprocess

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
    except Exception as e:
        print(f"❌ Error reading input files: {e}")
        return

    # Build uppercase lookup map
    team_map_df["uppercase"] = team_map_df["team_name"].str.strip().str.upper()
    team_map = dict(zip(team_map_df["uppercase"], team_map_df["team_name"]))

    # Normalize all team names in games + stadium to uppercase for join
    games_df["home_team"] = games_df["home_team"].str.strip().str.upper()
    games_df["away_team"] = games_df["away_team"].str.strip().str.upper()
    stadium_df["home_team"] = stadium_df["home_team"].str.strip().str.upper()

    # Drop conflicting column
    games_df = games_df.drop(columns=["game_time"], errors="ignore")

    # Merge stadium and game data
    merged = pd.merge(games_df, stadium_df, on="home_team", how="left")
    if merged.empty:
        print("❌ Merge failed: no matching home_team.")
        return

    # Replace home_team and away_team with proper casing
    merged["home_team"] = merged["home_team"].map(team_map).fillna(merged["home_team"])
    merged["away_team"] = merged["away_team"].map(team_map).fillna(merged["away_team"])

    # Save to file
    merged.to_csv(OUTPUT_FILE, index=False)

    summary = (
        f"✅ Weather input file generated\n"
        f"🔢 Rows: {len(merged)}\n"
        f"📁 Output: {OUTPUT_FILE}\n"
        f"📄 Games file: {GAMES_FILE}\n"
        f"🏟️ Stadium file: {STADIUM_FILE}"
    )
    print(summary)
    Path(SUMMARY_FILE).write_text(summary)

    try:
        subprocess.run(["git", "add", OUTPUT_FILE, SUMMARY_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "🔁 Fix team name casing in weather_input.csv"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed: {e}")

if __name__ == "__main__":
    generate_weather_csv()
