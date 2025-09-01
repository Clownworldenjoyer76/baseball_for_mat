#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

HOME_FILE = Path("data/adjusted/batters_home.csv")
AWAY_FILE = Path("data/adjusted/batters_away.csv")
GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")

def required(df: pd.DataFrame, cols, where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{where}: missing columns {missing}")

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    batters_home = load_csv(HOME_FILE)
    batters_away = load_csv(AWAY_FILE)
    games = load_csv(GAMES_FILE)

    required(batters_home, ["team"], str(HOME_FILE))
    required(batters_away, ["team"], str(AWAY_FILE))
    required(games, ["home_team", "away_team"], str(GAMES_FILE))

    games_for_merge = games[["home_team", "away_team"]].drop_duplicates()

    batters_home = pd.merge(
        batters_home,
        games_for_merge,
        left_on="team",
        right_on="home_team",
        how="left",
        suffixes=("_batter", ""),
    )

    batters_away = pd.merge(
        batters_away,
        games_for_merge,
        left_on="team",
        right_on="away_team",
        how="left",
        suffixes=("_batter", ""),
    )

    batters_home.drop_duplicates(inplace=True)
    batters_away.drop_duplicates(inplace=True)

    batters_home.to_csv(HOME_FILE, index=False)
    batters_away.to_csv(AWAY_FILE, index=False)
    print("✅ Corrected home_team and away_team values added to both files.")

if __name__ == "__main__":
    main()
