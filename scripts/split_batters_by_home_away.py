import pandas as pd
from pathlib import Path

BATTERS_FILE = "data/cleaned/batters_today.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
OUTPUT_DIR = "data/adjusted"

def main():
    print("📥 Loading input files...")
    batters = pd.read_csv(BATTERS_FILE)
    team_map = pd.read_csv(TEAM_MAP_FILE)
    games = pd.read_csv(GAMES_FILE)

    print("🔍 Validating required columns...")
    if 'team' not in batters.columns:
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'team_name' not in team_map.columns or 'abbreviation' not in team_map.columns:
        raise ValueError("team_name_master.csv missing required columns.")
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")

    print(f"✅ team_name_master.csv loaded with columns: {team_map.columns.tolist()}")

    print("🔗 Merging team names to codes...")
    batters = batters.merge(team_map, how='left', left_on='team', right_on='team_name')
    batters.drop(columns=['team_name'], inplace=True)
    batters.rename(columns={'abbreviation': 'team_code'}, inplace=True)

    home_teams = games['home_team'].unique()
    away_teams = games['away_team'].unique()

    print("⚙️ Splitting batters into home and away...")
    home_batters = batters[batters['team_code'].isin(home_teams)]
    away_batters = batters[batters['team_code'].isin(away_teams)]

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    home_batters.to_csv(f"{OUTPUT_DIR}/batters_home.csv", index=False)
    away_batters.to_csv(f"{OUTPUT_DIR}/batters_away.csv", index=False)

    print(f"✅ Saved {len(home_batters)} home batters and {len(away_batters)} away batters")

if __name__ == "__main__":
    main()
