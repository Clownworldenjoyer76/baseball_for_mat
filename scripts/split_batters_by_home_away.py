import pandas as pd
from pathlib import Path
import re

BATTERS_FILE = "data/cleaned/batters_today.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MASTER_FILE = "data/Data/team_name_master.csv"
OUTPUT_DIR = "data/adjusted"

def fix_team_name(name):
    """Fix camel case like 'RedSox' → 'Red Sox' and remove extra whitespace."""
    name = str(name).strip()
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', name)

def normalize_team(name, valid_teams):
    name = str(name).strip().lower()
    for valid in valid_teams:
        if name == valid.lower():
            return valid
    return name.title()

def main():
    print("📥 Loading input files...")
    batters = pd.read_csv(BATTERS_FILE)
    games = pd.read_csv(GAMES_FILE)
    team_master = pd.read_csv(TEAM_MASTER_FILE)
    valid_teams = team_master["team_name"].dropna().tolist()

    print("🔍 Validating required columns...")
    if 'team' not in batters.columns:
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")

    print("🧹 Cleaning and formatting team names...")
    batters['team'] = batters['team'].apply(fix_team_name)
    batters['team'] = batters['team'].apply(lambda x: normalize_team(x, valid_teams))

    games['home_team'] = games['home_team'].astype(str).str.strip().str.title()
    games['away_team'] = games['away_team'].astype(str).str.strip().str.title()

    home_teams = games['home_team'].unique()
    away_teams = games['away_team'].unique()

    print(f"🏠 Home teams: {sorted(home_teams)}")
    print(f"🛫 Away teams: {sorted(away_teams)}")

    batter_teams = sorted(batters['team'].unique())
    print(f"👕 Batter teams: {batter_teams}")

    unmatched_teams = [team for team in batter_teams if team not in home_teams and team not in away_teams]
    if unmatched_teams:
        print(f"⚠️ Unmatched batter teams (not in today's games): {unmatched_teams}")
    else:
        print("✅ All batter teams matched to today's games.")

    print("⚙️ Splitting batters into home and away...")
    home_batters = batters[batters['team'].isin(home_teams)]
    away_batters = batters[batters['team'].isin(away_teams)]

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    home_batters.to_csv(f"{OUTPUT_DIR}/batters_home.csv", index=False)
    away_batters.to_csv(f"{OUTPUT_DIR}/batters_away.csv", index=False)

    print(f"✅ Saved {len(home_batters)} home batters and {len(away_batters)} away batters")

if __name__ == "__main__":
    main()
