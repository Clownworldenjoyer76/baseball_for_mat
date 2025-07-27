import pandas as pd
from pathlib import Path

# Input files
HOME_FILE = Path("data/adjusted/batters_home.csv")
AWAY_FILE = Path("data/adjusted/batters_away.csv")
GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    # Load data
    batters_home = load_csv(HOME_FILE)
    batters_away = load_csv(AWAY_FILE)
    games = load_csv(GAMES_FILE)

    # Normalize team names for merging
    games = games[['home_team', 'away_team']].drop_duplicates()

    # Add columns to away batters
    away_team = batters_away['team'].unique()
    if len(away_team) == 1:
        away = away_team[0]
        match = games[games['away_team'] == away]
        if not match.empty:
            batters_away['home_team'] = match['home_team'].values[0]
            batters_away['away_team'] = away

    # Add columns to home batters
    home_team = batters_home['team'].unique()
    if len(home_team) == 1:
        home = home_team[0]
        match = games[games['home_team'] == home]
        if not match.empty:
            batters_home['away_team'] = match['away_team'].values[0]
            batters_home['home_team'] = home

    # Save updated files
    batters_home.to_csv(HOME_FILE, index=False)
    batters_away.to_csv(AWAY_FILE, index=False)

    print("✅ Corrected home_team and away_team values added to both files.")

if __name__ == "__main__":
    main()
