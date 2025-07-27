import pandas as pd
from pathlib import Path

AWAY_FILE = Path("data/adjusted/pitchers_away.csv")
HOME_FILE = Path("data/adjusted/pitchers_home.csv")

def cleanup_away():
    df = pd.read_csv(AWAY_FILE)

    # Replace name column with 'last_name, first_name'
    if "last_name" in df.columns and "first_name" in df.columns:
        df["name"] = df["last_name"].astype(str).str.strip() + ", " + df["first_name"].astype(str).str.strip()

    # Drop and rename columns
    if "game_away_team" in df.columns:
        df.drop(columns=["game_away_team"], inplace=True)
    if "game_home_team" in df.columns:
        df.rename(columns={"game_home_team": "home_team"}, inplace=True)

    df.to_csv(AWAY_FILE, index=False)
    print(f"✅ Cleaned: {AWAY_FILE}")

def cleanup_home():
    df = pd.read_csv(HOME_FILE)

    # Replace name column with 'last_name, first_name'
    if "last_name" in df.columns and "first_name" in df.columns:
        df["name"] = df["last_name"].astype(str).str.strip() + ", " + df["first_name"].astype(str).str.strip()

    # Drop and rename columns
    if "game_home_team" in df.columns:
        df.drop(columns=["game_home_team"], inplace=True)
    if "game_away_team" in df.columns:
        df.rename(columns={"game_away_team": "away_team"}, inplace=True)

    df.to_csv(HOME_FILE, index=False)
    print(f"✅ Cleaned: {HOME_FILE}")

def main():
    cleanup_away()
    cleanup_home()

if __name__ == "__main__":
    main()
