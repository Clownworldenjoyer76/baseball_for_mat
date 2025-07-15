
import pandas as pd
from pathlib import Path

BATTERS_FILE = "data/cleaned/batters_today.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
TEAM_MAP_FILE = "data/Data/team_abv_map.csv"
OUTPUT_DIR = "data/adjusted"

def main():
    batters = pd.read_csv(BATTERS_FILE)
    games = pd.read_csv(GAMES_FILE)
    team_map = pd.read_csv(TEAM_MAP_FILE)

    # Confirm required columns
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        raise ValueError("Missing 'home_team' or 'away_team' in games file.")
    if 'Abbreviation' not in team_map.columns or 'Team' not in team_map.columns:
        raise ValueError("team_abv_map.csv missing required columns.")

    # Map abbreviation → full name
    abv_to_name = dict(zip(team_map['Abbreviation'], team_map['Team']))
    batters['full_team_name'] = batters['team'].map(abv_to_name)

    # Filter based on full team names
    home_teams = games['home_team'].unique()
    away_teams = games['away_team'].unique()

    home_batters = batters[batters['full_team_name'].isin(home_teams)].drop(columns=['full_team_name'])
    away_batters = batters[batters['full_team_name'].isin(away_teams)].drop(columns=['full_team_name'])

    # Output
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    home_batters.to_csv(f"{OUTPUT_DIR}/batters_home.csv", index=False)
    away_batters.to_csv(f"{OUTPUT_DIR}/batters_away.csv", index=False)

    print(f"✅ Saved {len(home_batters)} home batters and {len(away_batters)} away batters")

if __name__ == "__main__":
    main()
