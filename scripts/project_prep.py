#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

DATA_DIR   = Path("data")
OUTPUT_DIR = DATA_DIR / "end_chain" / "final"
RAW_DIR    = DATA_DIR / "raw"

ID_FORCE = [
    "home_team_id","away_team_id","pitcher_home_id","pitcher_away_id",
    "player_id","team_id","game_id"
]

def _require(df: pd.DataFrame, cols: list[str], name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise KeyError(f"{name} missing required columns: {miss}")

def _to_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def project_prep():
    todays_games  = DATA_DIR / "_projections" / "todaysgames_normalized_fixed.csv"
    pitchers_file = DATA_DIR / "Data" / "pitchers.csv"
    stadiums_file = DATA_DIR / "manual" / "stadium_master.csv"

    if not todays_games.exists():  raise FileNotFoundError(f"Missing input: {todays_games}")
    if not pitchers_file.exists(): raise FileNotFoundError(f"Missing input: {pitchers_file}")
    if not stadiums_file.exists(): raise FileNotFoundError(f"Missing input: {stadiums_file}")

    # Read as strings; prevent NA tokenization so we don't get "nan" on output
    games    = pd.read_csv(todays_games,  dtype=str, low_memory=False, keep_default_na=False)
    pitchers = pd.read_csv(pitchers_file, dtype=str, low_memory=False, keep_default_na=False)
    stadiums = pd.read_csv(stadiums_file, dtype=str, low_memory=False, keep_default_na=False)

    # Normalize headers
    games.columns    = [c.strip() for c in games.columns]
    pitchers.columns = [c.strip() for c in pitchers.columns]
    stadiums.columns = [c.strip() for c in stadiums.columns]

    _require(games,    ["home_team_id","away_team_id","pitcher_home_id","pitcher_away_id"], "games")
    _require(pitchers, ["player_id"], "pitchers")
    _require(stadiums, ["team_id"],   "stadiums")

    # Belt-and-suspenders: force known IDs to str
    games    = _to_str(games, ID_FORCE)
    pitchers = _to_str(pitchers, ID_FORCE)
    stadiums = _to_str(stadiums, ID_FORCE)

    # Merge pitcher identities by player_id (home/away)
    merged = games.merge(
        pitchers.add_suffix("_home"),
        left_on="pitcher_home_id",
        right_on="player_id_home",
        how="left",
    ).merge(
        pitchers.add_suffix("_away"),
        left_on="pitcher_away_id",
        right_on="player_id_away",
        how="left",
    )

    # ------- Stadium join (hardened) -------
    venue_cols_pref = ["team_id","team_name","venue","city","state","timezone","is_dome","latitude","longitude","home_team"]
    venue_cols = [c for c in venue_cols_pref if c in stadiums.columns]

    # Prepare a stable subset and lock dtypes
    stadium_sub = stadiums[venue_cols].copy()
    stadium_sub["team_id"] = stadium_sub["team_id"].astype(str)

    # Rename team_id -> home_team_id so we can merge with on="home_team_id"
    stadium_sub = stadium_sub.rename(columns={"team_id": "home_team_id"})

    # Lock the left key as string immediately before merge
    merged["home_team_id"] = merged["home_team_id"].astype(str)

    # Merge using identical key name on both sides to avoid coercion logic
    merged = merged.merge(
        stadium_sub,
        on="home_team_id",
        how="left",
        suffixes=("", "_stadium"),
    )
    # ---------------------------------------

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    out1 = OUTPUT_DIR / "startingpitchers.csv"
    out2 = RAW_DIR / "startingpitchers_with_opp_context.csv"

    # Ensure ID columns are strings on output
    merged = _to_str(merged, [
        "player_id_home","player_id_away","home_team_id","away_team_id","game_id",
        "pitcher_home_id","pitcher_away_id"
    ])

    merged.to_csv(out1, index=False)
    merged.to_csv(out2, index=False)
    print(f"project_prep: wrote {out1} and {out2} (rows={len(merged)})")

if __name__ == "__main__":
    project_prep()
