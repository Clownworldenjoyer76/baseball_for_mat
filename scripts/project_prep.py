#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_DIR = DATA_DIR / "end_chain" / "final"
RAW_DIR = DATA_DIR / "raw"

def project_prep():
    # Input paths
    todays_games = DATA_DIR / "_projections" / "todaysgames_normalized_fixed.csv"
    pitchers_file = DATA_DIR / "pitchers.csv"
    stadiums_file = DATA_DIR / "stadiums.csv"

    if not todays_games.exists():
        raise FileNotFoundError(f"Missing input: {todays_games}")
    if not pitchers_file.exists():
        raise FileNotFoundError(f"Missing input: {pitchers_file}")
    if not stadiums_file.exists():
        raise FileNotFoundError(f"Missing input: {stadiums_file}")

    # Load CSVs
    games = pd.read_csv(todays_games)
    pitchers = pd.read_csv(pitchers_file)
    stadiums = pd.read_csv(stadiums_file)

    # Clean column names
    games.columns = [c.strip() for c in games.columns]
    pitchers.columns = [c.strip() for c in pitchers.columns]
    stadiums.columns = [c.strip() for c in stadiums.columns]

    # Ensure consistent dtypes for IDs
    for col in ["home_team_id", "away_team_id", "team_id"]:
        if col in games.columns:
            games[col] = games[col].astype(str)
        if col in pitchers.columns:
            pitchers[col] = pitchers[col].astype(str)
        if col in stadiums.columns:
            stadiums[col] = stadiums[col].astype(str)

    if "game_id" in games.columns:
        games["game_id"] = games["game_id"].astype(str)
    if "game_id" in pitchers.columns:
        pitchers["game_id"] = pitchers["game_id"].astype(str)

    # Merge pitchers (home/away)
    merged = games.merge(
        pitchers.add_suffix("_home"),
        left_on=["home_team_id", "pitcher_home_id"],
        right_on=["team_id_home", "player_id_home"],
        how="left"
    )
    merged = merged.merge(
        pitchers.add_suffix("_away"),
        left_on=["away_team_id", "pitcher_away_id"],
        right_on=["team_id_away", "player_id_away"],
        how="left"
    )

    # Merge stadium info (on team_id)
    venue_cols = ["team_id", "stadium", "city", "state", "timezone", "is_dome", "park_factor"]
    venue_cols = [c for c in venue_cols if c in stadiums.columns]

    merged = merged.merge(
        stadiums[venue_cols].drop_duplicates("team_id"),
        left_on="home_team_id",
        right_on="team_id",
        how="left",
        suffixes=("", "_stadium")
    )

    # Output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    out1 = OUTPUT_DIR / "startingpitchers.csv"
    out2 = RAW_DIR / "startingpitchers_with_opp_context.csv"

    merged.to_csv(out1, index=False)
    merged.to_csv(out2, index=False)

    print(f"✅ project_prep: wrote {out1} and {out2} (rows={len(merged)})")

if __name__ == "__main__":
    project_prep()
