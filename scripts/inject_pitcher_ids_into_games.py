#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import glob
import sys

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PRIMARY_MASTER = "data/processed/player_team_master.csv"  # must have: name, player_id
FALLBACK_GLOB = "data/team_csvs/pitchers_*.csv"          # must have: name, player_id
OUT_FILE = "data/raw/todaysgames_normalized.csv"         # in-place update

REQUIRED_GAMES_COLS = ["game_id", "pitcher_home", "pitcher_away"]

def norm_name(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip()
    return s

def load_primary() -> dict:
    df = pd.read_csv(PRIMARY_MASTER)
    missing = [c for c in ["name", "player_id"] if c not in df.columns]
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {PRIMARY_MASTER} missing columns: {missing}")
    df = df.dropna(subset=["name", "player_id"]).copy()
    df["name"] = df["name"].apply(norm_name)
    # Ensure numeric IDs stay numeric-like strings (no decimals)
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64").astype("string").replace({"<NA>": ""})
    return dict(zip(df["name"], df["player_id"]))

def load_fallback() -> dict:
    mapping = {}
    paths = sorted(glob.glob(FALLBACK_GLOB))
    for p in paths:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if not {"name", "player_id"}.issubset(df.columns):
            # Strict: skip files without required columns
            continue
        df = df.dropna(subset=["name", "player_id"]).copy()
        df["name"] = df["name"].apply(norm_name)
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64").astype("string").replace({"<NA>": ""})
        # Latest seen wins; names are keys
        mapping.update(dict(zip(df["name"], df["player_id"])))
    return mapping

def main():
    # Load games
    games = pd.read_csv(GAMES_FILE)
    missing = [c for c in REQUIRED_GAMES_COLS if c not in games.columns]
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {missing}")

    # Ensure target ID columns exist
    for col in ["pitcher_home_id", "pitcher_away_id"]:
        if col not in games.columns:
            games[col] = pd.NA

    # Normalize names
    games["pitcher_home"] = games["pitcher_home"].apply(norm_name)
    games["pitcher_away"] = games["pitcher_away"].apply(norm_name)

    # Load mappings
    primary = load_primary()
    fallback = load_fallback()

    # Inject IDs
    def inject_id(name: str, existing_id):
        # Accept existing IDs
        if pd.notna(existing_id) and str(existing_id).strip() != "":
            return pd.to_numeric(existing_id, errors="coerce").astype("Int64")
        if name == "" or name.lower() == "undecided":
            return pd.NA
        pid = primary.get(name)
        if pid is None or pid == "":
            pid = fallback.get(name, "")
        if pid == "":
            return pd.NA
        return pd.to_numeric(pid, errors="coerce").astype("Int64")

    games["pitcher_home_id"] = games.apply(lambda r: inject_id(r["pitcher_home"], r["pitcher_home_id"]), axis=1)
    games["pitcher_away_id"] = games.apply(lambda r: inject_id(r["pitcher_away"], r["pitcher_away_id"]), axis=1)

    # Write back (IDs as digit-only strings in CSV)
    for col in ["pitcher_home_id", "pitcher_away_id"]:
        games[col] = games[col].astype("Int64").astype("string").replace({"<NA>": ""})

    games.to_csv(OUT_FILE, index=False)

    # Minimal console summary (mobile-friendly)
    total = len(games)
    h_filled = games["pitcher_home_id"].ne("").sum()
    a_filled = games["pitcher_away_id"].ne("").sum()
    print(f"✅ Injected pitcher IDs -> {OUT_FILE} (home filled: {h_filled}/{total}, away filled: {a_filled}/{total})")

if __name__ == "__main__":
    main()
