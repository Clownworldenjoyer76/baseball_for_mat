# pit2.py

import pandas as pd
from pathlib import Path

# File paths
AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
GAMES_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
OUTPUT_FILE = AWAY_FILE  # Overwrite in place

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    print("🔄 Loading files...")
    bat_away = load_csv(AWAY_FILE)
    games = load_csv(GAMES_FILE)

    print("🔗 Mapping pitcher_home to bat_away4...")
    # Normalize team names
    bat_away["away_team"] = bat_away["away_team"].str.strip().str.lower()
    games["away_team"] = games["away_team"].str.strip().str.lower()

    # Merge pitcher_home from games into bat_away4
    merged = bat_away.merge(
        games[["away_team", "pitcher_home"]],
        on="away_team",
        how="left"
    )

    # Save result
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Updated file saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
