#!/usr/bin/env python3

import sys
from pathlib import Path
import pandas as pd
import numpy as np

SUM_DIR = Path("summaries/projections")
SUM_DIR.mkdir(parents=True, exist_ok=True)

# Inputs (with sensible fallbacks)
TG_CANDIDATES = [
    Path("data/_projections/todaysgames_normalized_fixed.csv"),
    Path("data/_projections/todaysgames_normalized.csv"),
    Path("data/raw/todaysgames_normalized.csv"),
]
MEGAZ_CANDIDATES = [
    Path("data/_projections/pitcher_mega_z_final.csv"),
    Path("data/end_chain/final/pitcher_mega_z_final.csv"),
]

COVERAGE_CSV = SUM_DIR / "mega_z_starter_coverage.csv"
MISSING_CSV = SUM_DIR / "mega_z_starter_missing.csv"
BUILD_LOG   = SUM_DIR / "mega_z_build_log.txt"


def first_existing(paths):
    for p in paths:
        if p.is_file():
            return p
    return None


def to_num(series):
    return pd.to_numeric(series, errors="coerce")


def load_todaysgames():
    tg_path = first_existing(TG_CANDIDATES)
    if tg_path is None:
        raise RuntimeError("No todaysgames file found in expected locations.")
    tg = pd.read_csv(tg_path)
    # Normalize required columns
    must = ["game_id", "pitcher_home_id", "pitcher_away_id", "home_team_id", "away_team_id"]
    missing = [c for c in must if c not in tg.columns]
    if missing:
        raise RuntimeError(f"{tg_path} missing columns: {missing}")
    tg["game_id"] = to_num(tg["game_id"])
    tg["pitcher_home_id"] = to_num(tg["pitcher_home_id"])
    tg["pitcher_away_id"] = to_num(tg["pitcher_away_id"])
    tg["home_team_id"] = to_num(tg["home_team_id"])
    tg["away_team_id"] = to_num(tg["away_team_id"])
    return tg_path, tg


def load_mega_z():
    mz_path = first_existing(MEGAZ_CANDIDATES)
    if mz_path is None:
        raise RuntimeError("No pitcher_mega_z_final.csv found in expected locations.")
    mz = pd.read_csv(mz_path)
    if "player_id" not in mz.columns:
        raise RuntimeError(f"{mz_path} missing column: player_id")
    mz["player_id"] = to_num(mz["player_id"])
    return mz_path, mz


def main():
    # Log header (simple text file so you have context in artifacts)
    with open(BUILD_LOG, "w", encoding="utf-8") as fh:
        fh.write("starter_coverage_guard: begin\n")

    tg_path, tg = load_todaysgames()
    mz_path, mz = load_mega_z()

    with open(BUILD_LOG, "a", encoding="utf-8") as fh:
        fh.write(f"loaded: {tg_path}\n")
        fh.write(f"loaded: {mz_path}\n")

    # Build expected starters dataframe (one row per starter)
    home = tg[["game_id", "home_team_id", "pitcher_home_id"]].rename(
        columns={"home_team_id": "team_id", "pitcher_home_id": "player_id"}
    )
    away = tg[["game_id", "away_team_id", "pitcher_away_id"]].rename(
        columns={"away_team_id": "team_id", "pitcher_away_id": "player_id"}
    )
    starters = pd.concat([home, away], ignore_index=True)

    # Keep only valid numeric IDs
    starters = starters.dropna(subset=["player_id", "game_id", "team_id"])
    starters["player_id"] = starters["player_id"].astype("Int64")
    starters["game_id"] = starters["game_id"].astype("Int64")
    starters["team_id"] = starters["team_id"].astype("Int64")

    # Deduplicate in case of re-runs/duplicates
    starters = starters.drop_duplicates(subset=["player_id", "game_id", "team_id"]).reset_index(drop=True)

    # Cross-check against mega_z by player_id only (your preferred key)
    mz_ids = set(mz["player_id"].dropna().astype("Int64").tolist())
    starters["in_mega_z"] = starters["player_id"].isin(mz_ids)

    # Write full coverage table
    starters.sort_values(["game_id", "team_id", "player_id"]).to_csv(COVERAGE_CSV, index=False)

    # Write missing table
    missing = starters.loc[~starters["in_mega_z"]].copy()
    missing.sort_values(["game_id", "team_id", "player_id"]).to_csv(MISSING_CSV, index=False)

    miss_n = int(missing.shape[0])
    with open(BUILD_LOG, "a", encoding="utf-8") as fh:
        fh.write(f"starters expected: {starters.shape[0]}\n")
        fh.write(f"starters missing:  {miss_n}\n")

    if miss_n > 0:
        raise RuntimeError(
            "Starter coverage failure: "
            f"{miss_n} starter(s) absent in pitcher_mega_z. "
            "See summaries/projections/mega_z_starter_coverage.csv and "
            "summaries/projections/mega_z_starter_missing.csv."
        )

    # Success
    with open(BUILD_LOG, "a", encoding="utf-8") as fh:
        fh.write("starter_coverage_guard: OK\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Make sure diagnostics exist even on failure
        SUM_DIR.mkdir(parents=True, exist_ok=True)
        if not COVERAGE_CSV.exists():
            pd.DataFrame(columns=["game_id", "team_id", "player_id", "in_mega_z"]).to_csv(COVERAGE_CSV, index=False)
        if not MISSING_CSV.exists():
            pd.DataFrame(columns=["game_id", "team_id", "player_id"]).to_csv(MISSING_CSV, index=False)
        with open(BUILD_LOG, "a", encoding="utf-8") as fh:
            fh.write(f"ERROR: {repr(e)}\n")
        raise
