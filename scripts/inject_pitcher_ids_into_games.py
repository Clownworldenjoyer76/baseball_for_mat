#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
inject_pitcher_ids_into_games.py

Inject pitcher_home_id and pitcher_away_id into todaysgames_normalized.csv
using player_team_master.csv and fallback rosters.
"""

import pandas as pd
from pathlib import Path

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
ROSTER_DIR = Path("data/team_csvs")
OUTPUT_FILE = GAMES_FILE  # overwrite in place

# Explicit overrides for known problem cases
OVERRIDE_IDS = {
    "Richardson, Simeon Woods": 680573,
    "Gipson-Long, Sawyer": 687830,
}

def load_master():
    if not MASTER_FILE.exists():
        raise FileNotFoundError(f"{MASTER_FILE} not found")
    df = pd.read_csv(MASTER_FILE)
    return df[["name", "player_id"]]

def load_rosters():
    roster_files = ROSTER_DIR.glob("pitchers_*.csv")
    frames = []
    for f in roster_files:
        try:
            df = pd.read_csv(f)
            if {"name", "player_id"}.issubset(df.columns):
                frames.append(df[["name", "player_id"]])
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["name", "player_id"])

def build_lookup():
    master = load_master()
    rosters = load_rosters()
    combined = pd.concat([master, rosters], ignore_index=True).drop_duplicates(subset=["name"])
    return dict(zip(combined["name"], combined["player_id"]))

def main():
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"{GAMES_FILE} not found")
    games = pd.read_csv(GAMES_FILE)

    lookup = build_lookup()

    def resolve_id(name, current_id):
        # Preserve existing if present
        if pd.notna(current_id) and str(current_id).strip() != "":
            return current_id
        # Override map first
        if name in OVERRIDE_IDS:
            return OVERRIDE_IDS[name]
        # Lookup by name
        return lookup.get(name, pd.NA)

    games["pitcher_home_id"] = games.apply(
        lambda r: resolve_id(r["pitcher_home"], r.get("pitcher_home_id", None)), axis=1
    )
    games["pitcher_away_id"] = games.apply(
        lambda r: resolve_id(r["pitcher_away"], r.get("pitcher_away_id", None)), axis=1
    )

    games.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Injected pitcher IDs -> {OUTPUT_FILE} (home_id non-null: {games['pitcher_home_id'].notna().sum()}, away_id non-null: {games['pitcher_away_id'].notna().sum()})")

if __name__ == "__main__":
    main()
