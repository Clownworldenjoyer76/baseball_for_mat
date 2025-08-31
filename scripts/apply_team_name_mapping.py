#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

# Inputs/Outputs
TEAM_DIR      = Path("data/manual/team_directory.csv")
GAMES_PATH    = Path("data/raw/todaysgames_normalized.csv")
STADIUM_PATH  = Path("data/Data/stadium_metadata.csv")

def _load_team_directory() -> pd.DataFrame:
    """
    Load data/manual/team_directory.csv and normalize headers.
    Provides mapping between team_name and abbreviation.
    """
    if not TEAM_DIR.exists():
        print(f"ERROR: missing {TEAM_DIR}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(TEAM_DIR)
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Accept common header variants
    name_cols = [c for c in df.columns if c in {"team name", "team_name", "name"}]
    id_cols   = [c for c in df.columns if c in {"team id", "team_id", "id"}]
    abv_cols  = [c for c in df.columns if c in {"abbreviation", "abbr", "abbrev"}]

    if not name_cols or not id_cols or not abv_cols:
        print("ERROR: team_directory.csv must contain name/id/abbreviation columns.", file=sys.stderr)
        sys.exit(1)

    df = df.rename(columns={
        name_cols[0]: "team_name",
        id_cols[0]: "team_id",
        abv_cols[0]: "abbreviation",
    })

    df["team_name"] = df["team_name"].astype(str)
    df["abbreviation"] = df["abbreviation"].astype(str)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")

    df["_team_name_upper"] = df["team_name"].str.upper()
    return df[["team_name", "team_id", "abbreviation", "_team_name_upper"]]

def _map_to_abbr(series: pd.Series, name_to_abbr: dict) -> pd.Series:
    s = series.astype(str)
    return s.str.upper().map(name_to_abbr).fillna(s)

def _update_games(teams: pd.DataFrame) -> None:
    if not GAMES_PATH.exists():
        return

    g = pd.read_csv(GAMES_PATH)
    name_to_abbr = dict(zip(teams["_team_name_upper"], teams["abbreviation"]))

    # If home/away already abbreviations, mapping is no-op.
    for col in ("home_team", "away_team"):
        if col in g.columns:
            g[col] = _map_to_abbr(g[col], name_to_abbr)

    GAMES_PATH.parent.mkdir(parents=True, exist_ok=True)
    g.to_csv(GAMES_PATH, index=False)
    print(f"Updated: {GAMES_PATH} (if present)")

def _update_stadium(teams: pd.DataFrame) -> None:
    if not STADIUM_PATH.exists():
        return

    s = pd.read_csv(STADIUM_PATH)
    if "team" not in s.columns:
        # Nothing to normalize against; write back unchanged.
        s.to_csv(STADIUM_PATH, index=False)
        print(f"Updated: {STADIUM_PATH} (if present)")
        return

    name_to_abbr = dict(zip(teams["_team_name_upper"], teams["abbreviation"]))
    s["team"] = _map_to_abbr(s["team"], name_to_abbr)

    s.to_csv(STADIUM_PATH, index=False)
    print(f"Updated: {STADIUM_PATH} (if present)")

def main():
    teams = _load_team_directory()
    _update_games(teams)
    _update_stadium(teams)

if __name__ == "__main__":
    main()
