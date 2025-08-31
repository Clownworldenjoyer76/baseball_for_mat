#!/usr/bin/env python3
# Purpose: Normalize todaysgames to team abbreviations AND attach MLB numeric IDs.
# Inputs:
#   - data/raw/todaysgames.csv               (from scripts/todaysgames.py)
#   - data/manual/team_directory.csv         (exact headers: Team Name, Team ID, Abbreviation)
# Output:
#   - data/raw/todaysgames_normalized.csv    (adds home_team_id, away_team_id; ensures abbreviations)

import pandas as pd
from pathlib import Path

INPUT  = Path("data/raw/todaysgames.csv")
OUTPUT = Path("data/raw/todaysgames_normalized.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")

def load_team_directory() -> pd.DataFrame:
    # Expect exact headers: Team Name, Team ID, Abbreviation
    df = pd.read_csv(TEAM_DIR)
    required = {"Team Name", "Team ID", "Abbreviation"}
    if set(df.columns) != required and not required.issubset(set(df.columns)):
        raise ValueError("team_directory.csv must have exact headers: Team Name, Team ID, Abbreviation")
    return df[["Team Name", "Team ID", "Abbreviation"]].copy()

def make_maps(df: pd.DataFrame):
    # Two lookup paths:
    # 1) Full team name (upper) -> (abbr, id)
    # 2) Abbreviation (upper)   -> (abbr, id)  (so already-abbrev inputs still resolve)
    name_to = {
        str(row["Team Name"]).upper(): (str(row["Abbreviation"]), int(row["Team ID"]))
        for _, row in df.iterrows()
    }
    abbr_to = {
        str(row["Abbreviation"]).upper(): (str(row["Abbreviation"]), int(row["Team ID"]))
        for _, row in df.iterrows()
    }
    return name_to, abbr_to

def resolve(team_str: str, name_to, abbr_to):
    if not isinstance(team_str, str):
        return team_str, None
    key = team_str.strip().upper()
    if key in abbr_to:
        abbr, tid = abbr_to[key]
        return abbr, tid
    if key in name_to:
        abbr, tid = name_to[key]
        return abbr, tid
    # Fallback: leave as-is, id unknown
    return team_str, None

def normalize():
    # Load inputs
    games = pd.read_csv(INPUT)
    teams = load_team_directory()
    name_to, abbr_to = make_maps(teams)

    # Resolve home/away to (abbr, id)
    home_abbr, home_id = [], []
    away_abbr, away_id = [], []
    for _, r in games.iterrows():
        h_abbr, h_id = resolve(r.get("home_team"), name_to, abbr_to)
        a_abbr, a_id = resolve(r.get("away_team"), name_to, abbr_to)
        home_abbr.append(h_abbr)
        home_id.append(h_id)
        away_abbr.append(a_abbr)
        away_id.append(a_id)

    # Write back normalized values
    out = games.copy()
    out["home_team"] = home_abbr
    out["away_team"] = away_abbr
    out["home_team_id"] = pd.Series(home_id, dtype="Int64")
    out["away_team_id"] = pd.Series(away_id, dtype="Int64")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT, index=False)
    print(f"✅ normalize_todays_games wrote {len(out)} rows -> {OUTPUT}")

if __name__ == "__main__":
    normalize()
