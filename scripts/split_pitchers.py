import pandas as pd
from pathlib import Path

PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
OUTPUT_DIR = "data/adjusted"

def main():
    print("📥 Loading input files...")
    pitchers = pd.read_csv(PITCHERS_FILE)
    games = pd.read_csv(GAMES_FILE)

    print("🧹 Normalizing team and pitcher names...")
    pitchers['team'] = pitchers['team'].astype(str).str.strip().str.lower()
    pitchers['name'] = pitchers['name'].astype(str).str.strip().str.lower()
    games['home_team'] = games['home_team'].astype(str).str.strip().str.lower()
    games['away_team'] = games['away_team'].astype(str).str.strip().str.lower()
    games['pitcher_home'] = games['pitcher_home'].astype(str).str.strip().str.lower()
    games['pitcher_away'] = games['pitcher_away'].astype(str).str.strip().str.lower()

    print("🔍 Filtering pitchers by matchups...")
    home_pitchers = pd.merge(
        games[['home_team', 'pitcher_home']],
        pitchers,
        left_on=['home_team', 'pitcher_home'],
        right_on=['team', 'name'],
        how='inner'
    )

    away_pitchers = pd.merge(
        games[['away_team', 'pitcher_away']],
        pitchers,
        left_on=['away_team', 'pitcher_away'],
        right_on=['team', 'name'],
        how='inner'
    )

    print("💾 Saving results...")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    home_pitchers.to_csv(f"{OUTPUT_DIR}/pitchers_home.csv", index=False)
    away_pitchers.to_csv(f"{OUTPUT_DIR}/pitchers_away.csv", index=False)

    print(f"✅ Saved {len(home_pitchers)} home pitchers and {len(away_pitchers)} away pitchers.")

if __name__ == "__main__":
    main()
