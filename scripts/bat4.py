import pandas as pd
from pathlib import Path

# File paths
BAT_AWAY_PATH = Path("data/end_chain/final/updating/bat_away3.csv")
GAMES_PATH = Path("data/end_chain/cleaned/games_today_cleaned.csv")
OUTPUT_PATH = Path("data/end_chain/final/updating/bat_away4.csv")

def normalize_team_name(name):
    if not isinstance(name, str):
        return ""
    return name.strip().lower().title()

def main():
    # Load files
    try:
        bat = pd.read_csv(BAT_AWAY_PATH)
        games = pd.read_csv(GAMES_PATH)
    except Exception as e:
        print(f"❌ Error loading files: {e}")
        return

    # Normalize away_team in both DataFrames
    bat["away_team_norm"] = bat["away_team"].apply(normalize_team_name)
    games["away_team_norm"] = games["away_team"].apply(normalize_team_name)

    # Merge pitcher_away into bat DataFrame
    merged = pd.merge(
        bat,
        games[["away_team_norm", "pitcher_away"]],
        on="away_team_norm",
        how="left"
    )

    if "pitcher_away" not in merged.columns:
        print("❌ Failed to inject 'pitcher_away'. Check for team name mismatches.")
        return

    # Drop helper column
    merged.drop(columns=["away_team_norm"], inplace=True)

    # Save result
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Fixed bat file written to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
