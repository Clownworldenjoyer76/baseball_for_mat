#!/usr/bin/env python3
import pandas as pd
import argparse
import sys
from pathlib import Path

DEFAULT_GAMES_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")

def main():
    parser = argparse.ArgumentParser(description="Attach pitcher IDs to games file.")
    parser.add_argument("--games", type=str, default=str(DEFAULT_GAMES_FILE), help="Path to games_today_cleaned.csv")
    args = parser.parse_args()

    games_file = Path(args.games)
    if not games_file.exists():
        print(f"❌ Games file not found: {games_file}")
        sys.exit(1)

    games_df = pd.read_csv(games_file)
    required_pitcher_cols = ["home_pitcher_id", "away_pitcher_id"]
    for col in required_pitcher_cols:
        if col not in games_df.columns:
            print(f"❌ Missing required column: {col}")
            sys.exit(1)

    missing_ids = games_df[required_pitcher_cols].isna().sum()
    if missing_ids.any():
        print(f"❌ Missing pitcher IDs: {missing_ids.to_dict()}")
        sys.exit(1)

    print(f"✅ All pitcher IDs present in {games_file}")

if __name__ == "__main__":
    main()
