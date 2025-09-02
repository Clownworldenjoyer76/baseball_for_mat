#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
from unidecode import unidecode
import glob
import sys

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
ROSTERS_GLOB = "data/rosters/ready/p_*.csv"

REQ_GAMES_COLS = {"pitcher_home", "pitcher_away"}
REQ_MASTER_COLS = {"name", "player_id"}
REQ_ROSTER_COLS = {"name", "player_id"}

def norm_name(s):
    if pd.isna(s):
        return ""
    return unidecode(str(s)).strip()

def load_master_map():
    if not MASTER_FILE.exists():
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {MASTER_FILE} not found")
    df = pd.read_csv(MASTER_FILE)
    missing = REQ_MASTER_COLS - set(df.columns)
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {MASTER_FILE} missing columns: {sorted(missing)}")
    df["name_key"] = df["name"].apply(norm_name).str.casefold()
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["name_key", "player_id"])
    # Keep first occurrence per name_key
    df = df.drop_duplicates(subset=["name_key"])
    return dict(zip(df["name_key"], df["player_id"]))

def load_roster_fallback_map():
    mapping = {}
    paths = sorted(glob.glob(ROSTERS_GLOB))
    for p in paths:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if not REQ_ROSTER_COLS.issubset(df.columns):
            # Skip silently if schema doesn’t match
            continue
        df["name_key"] = df["name"].apply(norm_name).str.casefold()
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["name_key", "player_id"]).drop_duplicates(subset=["name_key"])
        for k, v in zip(df["name_key"], df["player_id"]):
            # Do not overwrite if already present
            if k and k not in mapping:
                mapping[k] = v
    return mapping

def main():
    if not GAMES_FILE.exists():
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} not found")

    games = pd.read_csv(GAMES_FILE)
    missing_games = REQ_GAMES_COLS - set(games.columns)
    if missing_games:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(missing_games)}")

    # Ensure ID columns exist
    if "pitcher_home_id" not in games.columns:
        games["pitcher_home_id"] = pd.NA
    if "pitcher_away_id" not in games.columns:
        games["pitcher_away_id"] = pd.NA

    # Normalize name fields
    games["pitcher_home_norm"] = games["pitcher_home"].apply(norm_name)
    games["pitcher_away_norm"] = games["pitcher_away"].apply(norm_name)

    master_map = load_master_map()
    roster_map = load_roster_fallback_map()

    def resolve_id(name):
        if not name or name.lower() == "undecided":
            return pd.NA  # accepted empty
        key = unidecode(name).strip().casefold()
        if key in master_map:
            return master_map[key]
        if key in roster_map:
            return roster_map[key]
        return pd.NA  # unresolved is accepted

    # Resolve IDs
    games["pitcher_home_id"] = games["pitcher_home_norm"].apply(resolve_id).astype("Int64")
    games["pitcher_away_id"] = games["pitcher_away_norm"].apply(resolve_id).astype("Int64")

    # Clean temp cols
    games.drop(columns=["pitcher_home_norm", "pitcher_away_norm"], inplace=True)

    # Persist (nullable Int64 saves without .0)
    GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_FILE, index=False)

    # Simple run summary
    total = len(games)
    h_nonnull = int(games["pitcher_home_id"].notna().sum())
    a_nonnull = int(games["pitcher_away_id"].notna().sum())
    print(f"✅ Injected pitcher IDs -> {GAMES_FILE} (home_id non-null: {h_nonnull}/{total}, away_id non-null: {a_nonnull}/{total})")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e))
        sys.exit(1)
