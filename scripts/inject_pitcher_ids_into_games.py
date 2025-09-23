#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/inject_pitcher_ids_into_games.py

import pandas as pd
from pathlib import Path
import glob

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
TEAM_PITCHERS_GLOB = "data/team_csvs/pitchers_*.csv"

# Hard overrides (authoritative)
OVERRIDES = {
    "Richardson, Simeon Woods": 680573,
    "Gipson-Long, Sawyer": 687830,
    "Berríos, José": 621244,
}

def load_games() -> pd.DataFrame:
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"{GAMES_FILE} not found")
    df = pd.read_csv(GAMES_FILE)
    req = {"game_id", "home_team", "away_team", "pitcher_home", "pitcher_away"}
    missing = req - set(df.columns)
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(missing)}")
    # Ensure id columns exist for passthrough
    for col in ("pitcher_home_id", "pitcher_away_id"):
        if col not in df.columns:
            df[col] = pd.NA
    return df

def build_name_to_id() -> dict:
    name_to_id: dict[str, int] = {}

    # 1) master file
    if MASTER_FILE.exists():
        mf = pd.read_csv(MASTER_FILE)
        if {"name", "player_id"}.issubset(mf.columns):
            mf = mf.dropna(subset=["name", "player_id"])
            for _, r in mf.iterrows():
                try:
                    pid = int(pd.to_numeric(r["player_id"], errors="coerce"))
                except Exception:
                    continue
                name_to_id[str(r["name"]).strip()] = pid

    # 2) team pitchers files
    for p in glob.glob(TEAM_PITCHERS_GLOB):
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if {"name", "player_id"}.issubset(df.columns):
            df = df.dropna(subset=["name", "player_id"])
            for _, r in df.iterrows():
                try:
                    pid = int(pd.to_numeric(r["player_id"], errors="coerce"))
                except Exception:
                    continue
                name_to_id[str(r["name"]).strip()] = pid

    # 3) overrides (authoritative)
    name_to_id.update(OVERRIDES)

    return name_to_id

def resolve_id(pitcher_name: str, existing_id):
    # Leave "Undecided" as missing
    if isinstance(pitcher_name, str) and pitcher_name.strip().lower() == "undecided":
        return pd.NA

    # If already present, keep
    if pd.notna(existing_id):
        return existing_id

    # Lookup by exact name
    pid = NAME_TO_ID.get(str(pitcher_name).strip())
    return pid if pid is not None else pd.NA

def main():
    games = load_games()
    global NAME_TO_ID
    NAME_TO_ID = build_name_to_id()

    # Inject IDs
    games["pitcher_home_id"] = games.apply(
        lambda r: resolve_id(r["pitcher_home"], r.get("pitcher_home_id", pd.NA)), axis=1
    )
    games["pitcher_away_id"] = games.apply(
        lambda r: resolve_id(r["pitcher_away"], r.get("pitcher_away_id", pd.NA)), axis=1
    )

    # Normalize dtype to pandas nullable Int64
    for col in ("pitcher_home_id", "pitcher_away_id"):
        games[col] = pd.to_numeric(games[col], errors="coerce").astype("Int64")

    # Preserve all passthrough columns and write back
    GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_FILE, index=False)

if __name__ == "__main__":
    main()
