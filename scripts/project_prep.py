#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

DATA_DIR   = Path("data")
OUTPUT_DIR = DATA_DIR / "end_chain" / "final"
RAW_DIR    = DATA_DIR / "raw"

def _require(df: pd.DataFrame, cols: list, name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise KeyError(f"{name} missing required columns: {miss}")

def _to_str(s: pd.Series) -> pd.Series:
    # Keep NaNs as empty strings for consistency in downstream merges
    return s.astype("string").fillna("")

def project_prep():
    todays_games  = DATA_DIR / "_projections" / "todaysgames_normalized_fixed.csv"
    pitchers_file = DATA_DIR / "Data" / "pitchers.csv"
    stadiums_file = DATA_DIR / "manual" / "stadium_master.csv"

    if not todays_games.exists():  raise FileNotFoundError(f"Missing input: {todays_games}")
    if not pitchers_file.exists(): raise FileNotFoundError(f"Missing input: {pitchers_file}")
    if not stadiums_file.exists(): raise FileNotFoundError(f"Missing input: {stadiums_file}")

    games    = pd.read_csv(todays_games)
    pitchers = pd.read_csv(pitchers_file)
    stadiums = pd.read_csv(stadiums_file)

    # Trim column whitespace
    games.columns    = [c.strip() for c in games.columns]
    pitchers.columns = [c.strip() for c in pitchers.columns]
    stadiums.columns = [c.strip() for c in stadiums.columns]

    # Required columns
    _require(games,    ["home_team_id","away_team_id","pitcher_home_id","pitcher_away_id"], "games")
    _require(pitchers, ["player_id"], "pitchers")
    _require(stadiums, ["team_id"],   "stadiums")

    # Normalize dtypes for join keys (everything as strings per your standard)
    for col in ["home_team_id","away_team_id","pitcher_home_id","pitcher_away_id"]:
        games[col] = _to_str(games[col])
    pitchers["player_id"] = _to_str(pitchers["player_id"])
    stadiums["team_id"]   = _to_str(stadiums["team_id"])

    # Merge pitcher identities by player_id (keep both sides)
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

    # Stadium attributes by team_id (home park context)
    venue_cols_pref = [
        "team_id","team_name","venue","city","state","timezone","is_dome",
        "latitude","longitude","home_team"
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

    # === CRITICAL ADD ===
    # Downstream expects a plain `player_id`. Provide it explicitly.
    # Use the home starter id (string), which aligns with how downstream scripts key this file.
    merged["player_id"] = merged["pitcher_home_id"].astype("string").fillna("")

    # Also reaffirm ID columns as strings (no accidental int regressions)
    for col in ["team_id","home_team_id","away_team_id","player_id",
                "pitcher_home_id","pitcher_away_id"]:
        if col in merged.columns:
            merged[col] = _to_str(merged[col])

    # Output locations
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    out1 = OUTPUT_DIR / "startingpitchers.csv"
    out2 = RAW_DIR / "startingpitchers_with_opp_context.csv"

    merged.to_csv(out1, index=False)
    merged.to_csv(out2, index=False)
    print(f"project_prep: wrote {out1} and {out2} (rows={len(merged)})")

if __name__ == "__main__":
    project_prep()
