#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

# Paths (inputs and outputs)
TODAY_GAMES = Path("data/raw/todaysgames_normalized.csv")
TEAM_DIR    = Path("data/manual/team_directory.csv")
STADIUM_OUT = Path("data/Data/stadium_metadata.csv")  # keep existing output location

def _load_team_directory() -> pd.DataFrame:
    """
    Load data/manual/team_directory.csv and normalize headers.
    Requires columns representing team name, team id, abbreviation.
    """
    if not TEAM_DIR.exists():
        print(f"ERROR: missing {TEAM_DIR}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(TEAM_DIR)
    # Normalize headers to lowercase strings
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Supported header variants (header-agnostic)
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

    # Ensure types
    df["team_name"] = df["team_name"].astype(str)
    df["abbreviation"] = df["abbreviation"].astype(str)
    # team_id may be int-like; coerce to Int64 to tolerate blanks
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")

    # Build quick lookups
    df["_team_name_upper"] = df["team_name"].str.upper()
    return df[["team_name", "team_id", "abbreviation", "_team_name_upper"]]

def _load_today_games() -> pd.DataFrame:
    """
    Read normalized games file with home_team/away_team abbreviations.
    If absent, return empty DataFrame with expected columns.
    """
    cols = ["home_team", "away_team", "game_time"]
    if TODAY_GAMES.exists():
        g = pd.read_csv(TODAY_GAMES)
        # Ensure columns exist, fill if missing
        for c in cols:
            if c not in g.columns:
                g[c] = pd.NA
        return g[cols]
    else:
        return pd.DataFrame(columns=cols)

def _load_existing_stadium() -> pd.DataFrame:
    """
    Load existing stadium metadata (any columns). If missing, start empty with 'team' as a key.
    """
    if STADIUM_OUT.exists():
        s = pd.read_csv(STADIUM_OUT)
        # Guarantee 'team' column exists for keying (store as abbreviation)
        if "team" not in s.columns:
            s["team"] = pd.NA
        s["team"] = s["team"].astype(str)
        return s
    else:
        return pd.DataFrame(columns=["team"])

def main():
    teams = _load_team_directory()
    games = _load_today_games()
    stadium = _load_existing_stadium()

    # Home teams expected today (abbreviations if normalize_todays_games already ran)
    home_vals = (games["home_team"].dropna().astype(str).str.upper().unique().tolist()
                 if not games.empty else [])

    # Ensure a row exists for each home team abbreviation.
    # We do not invent columns; we only add minimal stub rows with 'team' populated if missing.
    if "team" not in stadium.columns:
        stadium["team"] = pd.NA

    stadium["team"] = stadium["team"].astype(str)
    have = set(stadium["team"].str.upper().tolist())
    need = [abbr for abbr in home_vals if abbr not in have]

    if need:
        add = pd.DataFrame({"team": need})
        stadium = pd.concat([stadium, add], ignore_index=True)

    # Sort for stability
    stadium = stadium.drop_duplicates(subset=["team"], keep="first")
    stadium = stadium.sort_values(by=["team"], kind="mergesort")

    # Write output
    STADIUM_OUT.parent.mkdir(parents=True, exist_ok=True)
    stadium.to_csv(STADIUM_OUT, index=False)
    print(f"Saved updated stadium metadata to {STADIUM_OUT}")

if __name__ == "__main__":
    main()
