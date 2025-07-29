# pit2.py

import pandas as pd
from pathlib import Path

# File paths
AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
GAMES_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
BATTERS_FILE = Path("data/cleaned/batters_today.csv")
BATTERS_OUTPUT = Path("data/end_chain/bat_today.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    bat_away = load_csv(AWAY_FILE)
    games = load_csv(GAMES_FILE)
    batters = load_csv(BATTERS_FILE)

    # Normalize team names
    bat_away["away_team"] = bat_away["away_team"].str.strip().str.lower()
    games["away_team"] = games["away_team"].str.strip().str.lower()

    # Merge pitcher_home into bat_away
    merged = bat_away.merge(
        games[["away_team", "pitcher_home"]],
        on="away_team",
        how="left"
    )
    merged.to_csv(AWAY_FILE, index=False)

    # Add "last_name, first_name" to batters_today
    batters["last_name, first_name"] = batters["name"]
    batters.to_csv(BATTERS_OUTPUT, index=False)

if __name__ == "__main__":
    main()
