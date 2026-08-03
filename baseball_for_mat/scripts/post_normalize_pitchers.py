#!/usr/bin/env python3
# scripts/post_normalize_pitchers.py
# Purpose: Fix pitcher projection outputs:
# - Ensure player_id present and standardized
# - Inject game_id from todaysgames
# - Write cleaned outputs

import pandas as pd
from pathlib import Path

# Inputs
PITCHER_PROPS = Path("data/_projections/pitcher_props_projected.csv")
PITCHER_MEGAZ = Path("data/_projections/pitcher_mega_z.csv")
GAMES_FILE    = Path("data/raw/todaysgames_normalized.csv")

# Outputs (overwrite in place)
OUT_PROPS = PITCHER_PROPS
OUT_MEGAZ = PITCHER_MEGAZ

def clean_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Strip/normalize player_id to string; drop blanks."""
    if "player_id" in df.columns:
        df["player_id"] = (
            df["player_id"].astype(str).str.strip().replace({"nan": "", "None": ""})
        )
    return df

def inject_game_ids(props: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    """Attach game_id to props using team and opponent matchup."""
    if "game_id" not in props.columns:
        props["game_id"] = ""

    if {"team", "game_id"}.issubset(games.columns):
        team_map = {}
        for _, r in games.iterrows():
            team_map[r["home_team"]] = r["game_id"]
            team_map[r["away_team"]] = r["game_id"]

        props["game_id"] = props["game_id"].where(
            props["game_id"].astype(str).str.len() > 0,
            props["team"].map(team_map).fillna(""),
        )
    return props

def process_file(path: Path, games: pd.DataFrame, inject_gameid: bool = False):
    if not path.exists():
        print(f"⚠️ Missing {path}, skipping.")
        return
    df = pd.read_csv(path)
    before = len(df)
    df = clean_ids(df)
    df = df[df["player_id"].astype(str).str.len() > 0].copy()
    if inject_gameid:
        df = inject_game_ids(df, games)
    after = len(df)
    df.to_csv(path, index=False)
    print(f"✅ Cleaned {path} ({after}/{before} rows kept)")

def main():
    games = pd.read_csv(GAMES_FILE) if GAMES_FILE.exists() else pd.DataFrame()

    # Props: must attach game_id
    process_file(PITCHER_PROPS, games, inject_gameid=True)

    # Mega-Z: no game_id needed, just clean IDs
    process_file(PITCHER_MEGAZ, games, inject_gameid=False)

if __name__ == "__main__":
    main()
