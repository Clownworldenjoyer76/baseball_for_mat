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
    # Load input data
    batters_home = load_csv(HOME_FILE)
    batters_away = load_csv(AWAY_FILE)
    games = load_csv(GAMES_FILE)

    # Extract just home_team and away_team columns for merging
    team_map = games[['home_team', 'away_team']].drop_duplicates()

    # Add to both files (every row gets the full values)
    batters_home['home_team'] = team_map['home_team'].values[0] if not team_map.empty else ""
    batters_home['away_team'] = team_map['away_team'].values[0] if not team_map.empty else ""

    batters_away['home_team'] = team_map['home_team'].values[0] if not team_map.empty else ""
    batters_away['away_team'] = team_map['away_team'].values[0] if not team_map.empty else ""

    # Save updated files
    batters_home.to_csv(HOME_FILE, index=False)
    batters_away.to_csv(AWAY_FILE, index=False)

    print("✅ Home and away team columns added.")

if __name__ == "__main__":
    main()
