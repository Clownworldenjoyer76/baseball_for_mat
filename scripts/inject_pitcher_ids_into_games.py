#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import os

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
PLAYER_MASTER_FILE = Path("data/processed/player_team_master.csv")

# Optional fallback dirs (kept as-is; will be used if present)
TEAM_CSV_DIR = Path("data/team_csvs")             # e.g., pitchers_Twins.csv
ROSTERS_READY_DIR = Path("data/rosters/ready")    # e.g., p_Tigers.csv

REQUIRED_GAME_COLS = [
    "game_id", "home_team", "away_team", "game_time",
    "pitcher_home", "pitcher_away", "home_team_id", "away_team_id"
]

def load_games(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_GAME_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {path} missing columns: {missing}")
    # Ensure passthrough ID columns exist
    if "pitcher_home_id" not in df.columns:
        df["pitcher_home_id"] = pd.NA
    if "pitcher_away_id" not in df.columns:
        df["pitcher_away_id"] = pd.NA
    return df

def load_player_master(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {path} not found")
    df = pd.read_csv(path)
    need = {"name", "player_id"}
    if not need.issubset(df.columns):
        missing = sorted(list(need - set(df.columns)))
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {path} missing columns: {missing}")
    # normalize
    df["name_norm"] = df["name"].astype(str).str.strip()
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    return df[["name_norm", "player_id"]].dropna(subset=["player_id"]).drop_duplicates()

def build_fallback_index() -> dict:
    idx = {}
    # TEAM_CSV_DIR fallback
    if TEAM_CSV_DIR.exists():
        for p in TEAM_CSV_DIR.glob("pitchers_*.csv"):
            try:
                tdf = pd.read_csv(p)
                if {"name", "player_id"}.issubset(tdf.columns):
                    tdf = tdf.copy()
                    tdf["name_norm"] = tdf["name"].astype(str).str.strip()
                    tdf["player_id"] = pd.to_numeric(tdf["player_id"], errors="coerce")
                    for _, r in tdf.dropna(subset=["player_id"]).iterrows():
                        nm = r["name_norm"]
                        if nm not in idx:
                            idx[nm] = int(r["player_id"])
            except Exception:
                pass
    # ROSTERS_READY_DIR fallback
    if ROSTERS_READY_DIR.exists():
        for p in ROSTERS_READY_DIR.glob("p_*.csv"):
            try:
                tdf = pd.read_csv(p)
                # accept common header variants
                name_col = "name" if "name" in tdf.columns else None
                pid_col = "player_id" if "player_id" in tdf.columns else None
                if name_col and pid_col:
                    tdf = tdf.copy()
                    tdf["name_norm"] = tdf[name_col].astype(str).str.strip()
                    tdf["player_id"] = pd.to_numeric(tdf[pid_col], errors="coerce")
                    for _, r in tdf.dropna(subset=["player_id"]).iterrows():
                        nm = r["name_norm"]
                        if nm not in idx:
                            idx[nm] = int(r["player_id"])
            except Exception:
                pass
    return idx

def make_lookup(master: pd.DataFrame) -> dict:
    base = dict(zip(master["name_norm"], master["player_id"].astype(int)))
    # supplement with fallbacks (if present)
    fall = build_fallback_index()
    base.update({k: v for k, v in fall.items() if k not in base})
    return base

def resolve_id(name_val, existing_id, lookup: dict):
    # Pass through existing if valid
    if pd.notna(existing_id):
        try:
            iv = pd.to_numeric(existing_id, errors="coerce")
            if pd.notna(iv):
                return int(iv)
        except Exception:
            pass
    # Undecided -> NA
    if pd.isna(name_val):
        return pd.NA
    name = str(name_val).strip()
    if name.lower() == "undecided":
        return pd.NA
    # Lookup by normalized name
    pid = lookup.get(name)
    if pid is None:
        return pd.NA
    return int(pid)

def main():
    games = load_games(GAMES_FILE)
    master = load_player_master(PLAYER_MASTER_FILE)
    lookup = make_lookup(master)

    # Compute Series without casting per-row
    home_ids = games.apply(lambda r: resolve_id(r["pitcher_home"], r.get("pitcher_home_id", pd.NA), lookup), axis=1)
    away_ids = games.apply(lambda r: resolve_id(r["pitcher_away"], r.get("pitcher_away_id", pd.NA), lookup), axis=1)

    # Now enforce nullable integer dtype on the full Series
    games["pitcher_home_id"] = pd.to_numeric(home_ids, errors="coerce").astype(pd.Int64Dtype())
    games["pitcher_away_id"] = pd.to_numeric(away_ids, errors="coerce").astype(pd.Int64Dtype())

    # Preserve passthrough columns; do NOT drop any columns
    # Just write back in current column order
    GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_FILE, index=False)

    # Minimal console info for CI logs
    non_null_home = int(games["pitcher_home_id"].notna().sum())
    non_null_away = int(games["pitcher_away_id"].notna().sum())
    print(f"✅ Injected pitcher IDs -> {GAMES_FILE} (home_id non-null: {non_null_home}, away_id non-null: {non_null_away})")

if __name__ == "__main__":
    main()
