#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# Directories
DATA_DIR = Path("data")
OUTPUT_DIR = DATA_DIR / "end_chain" / "final"
RAW_DIR = DATA_DIR / "raw"

def _require_columns(df: pd.DataFrame, cols: list, df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{df_name} missing required columns: {missing}")

def project_prep():
    # Inputs (confirmed paths)
    todays_games = DATA_DIR / "_projections" / "todaysgames_normalized_fixed.csv"
    pitchers_file = DATA_DIR / "Data" / "pitchers.csv"
    stadiums_file = DATA_DIR / "manual" / "stadium_master.csv"

    # Existence checks
    if not todays_games.exists():
        raise FileNotFoundError(f"Missing input: {todays_games}")
    if not pitchers_file.exists():
        raise FileNotFoundError(f"Missing input: {pitchers_file}")
    if not stadiums_file.exists():
        raise FileNotFoundError(f"Missing input: {stadiums_file}")

    # Load
    games = pd.read_csv(todays_games)
    pitchers = pd.read_csv(pitchers_file)
    stadiums = pd.read_csv(stadiums_file)

    # Normalize column names (strip whitespace, keep original casing otherwise)
    games.columns = [c.strip() for c in games.columns]
    pitchers.columns = [c.strip() for c in pitchers.columns]
    stadiums.columns = [c.strip() for c in stadiums.columns]

    # Required columns (confirmed from your uploads)
    _require_columns(
        games,
        ["home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"],
        "games",
    )
    _require_columns(pitchers, ["player_id"], "pitchers")
    _require_columns(stadiums, ["team_id"], "stadiums")

    # Types
    for c in ["home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]:
        games[c] = games[c].astype(str)
    pitchers["player_id"] = pitchers["player_id"].astype(str)
    stadiums["team_id"] = stadiums["team_id"].astype(str)

    # Merge pitchers by pitcher_id ONLY (no team_id dependency)
    merged = games.merge(
        pitchers.add_suffix("_home"),
        left_on="pitcher_home_id",
        right_on="player_id_home",
        how="left",
    )
    merged = merged.merge(
        pitchers.add_suffix("_away"),
        left_on="pitcher_away_id",
        right_on="player_id_away",
        how="left",
    )

    # Stadium info (join on confirmed ids)
    # Keep a safe subset if present
    venue_cols_pref = [
        "team_id", "team_name", "venue", "city", "state", "timezone", "is_dome",
        "latitude", "longitude", "home_team",
    ]
    venue_cols = [c for c in venue_cols_pref if c in stadiums.columns]
    stadium_sub = stadiums[venue_cols].drop_duplicates("team_id")
    merged = merged.merge(
        stadium_sub,
        left_on="home_team_id",
        right_on="team_id",
        how="left",
        suffixes=("", "_stadium"),
    )

    # Outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out1 = OUTPUT_DIR / "startingpitchers.csv"
    out2 = RAW_DIR / "startingpitchers_with_opp_context.csv"

    merged.to_csv(out1, index=False)
    merged.to_csv(out2, index=False)
    print(f"project_prep: wrote {out1} and {out2} (rows={len(merged)})")

if __name__ == "__main__":
    project_prep()
