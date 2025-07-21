# scripts/fix_away_team_column.py

import pandas as pd
from pathlib import Path

FILE_PATH = "data/adjusted/pitchers_away_park.csv"

def main():
    try:
        df = pd.read_csv(FILE_PATH)

        if "away_team_y" in df.columns:
            df = df.rename(columns={"away_team_y": "away_team"})
            df.to_csv(FILE_PATH, index=False)
            print("✅ Renamed 'away_team_y' to 'away_team' and saved to same file.")
        else:
            print("ℹ️ Column 'away_team_y' not found. No changes made.")

    except Exception as e:
        print(f"❌ Failed to process file: {e}")

if __name__ == "__main__":
    main()
