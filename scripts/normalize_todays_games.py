#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

INPUT = Path("data/raw/todaysgames.csv")
OUTPUT = Path("data/raw/todaysgames_normalized.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")

def load_team_directory():
    df = pd.read_csv(TEAM_DIR)
    df.columns = [c.strip().lower() for c in df.columns]
    return df.rename(columns={
        "team name": "team_name",
        "team id": "team_id",
        "abbreviation": "abbreviation"
    })

def normalize():
    games = pd.read_csv(INPUT)
    teams = load_team_directory()

    # Normalize home/away team names to canonical abbreviation
    mapping = dict(zip(teams["team_name"].str.upper(), teams["abbreviation"]))
    games["home_team"] = games["home_team"].str.upper().map(mapping).fillna(games["home_team"])
    games["away_team"] = games["away_team"].str.upper().map(mapping).fillna(games["away_team"])

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(OUTPUT, index=False)
    print(f"✅ normalize_todays_games wrote {len(games)} rows -> {OUTPUT}")

if __name__ == "__main__":
    normalize()
