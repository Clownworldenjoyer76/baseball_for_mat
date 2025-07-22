import pandas as pd
from pathlib import Path
import re
import subprocess

BATTERS_FILE = "data/cleaned/batters_today.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MASTER_FILE = "data/Data/team_name_master.csv"
OUTPUT_DIR = "data/adjusted"
OUTPUT_HOME = f"{OUTPUT_DIR}/batters_home.csv"
OUTPUT_AWAY = f"{OUTPUT_DIR}/batters_away.csv"

def fix_team_name(name):
    name = str(name).strip()
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', name)

def normalize_team(name, valid_teams):
    name = fix_team_name(name).lower()
    for team in valid_teams:
        if name == team.lower():
            return team
    return name

def main():
    print("📥 Loading input files...")
    batters = pd.read_csv(BATTERS_FILE)
    games = pd.read_csv(GAMES_FILE)
    team_master = pd.read_csv(TEAM_MASTER_FILE)
    valid_teams = team_master["team_name"].dropna().tolist()

    print("🧹 Cleaning and normalizing team names...")
    batters['team'] = batters['team'].apply(lambda x: normalize_team(x, valid_teams)).str.lower()
    games['home_team'] = games['home_team'].astype(str).str.strip().str.lower()
    games['away_team'] = games['away_team'].astype(str).str.strip().str.lower()

    home_teams = games['home_team'].unique()
    away_teams = games['away_team'].unique()

    print("⚙️ Splitting batters into home and away...")
    home_batters = batters[batters['team'].isin(home_teams)]
    away_batters = batters[batters['team'].isin(away_teams)]

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    home_batters.to_csv(OUTPUT_HOME, index=False)
    away_batters.to_csv(OUTPUT_AWAY, index=False)

    print(f"✅ Saved {len(home_batters)} home batters and {len(away_batters)} away batters")

    try:
        subprocess.run(["git", "add", OUTPUT_HOME, OUTPUT_AWAY], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: Split and normalized batters into home and away"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Files committed and pushed to repo.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git push failed: {e}")

if __name__ == "__main__":
    main()
