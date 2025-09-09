#!/usr/bin/env python3
# scripts/project_pitcher_props.py

import sys
from pathlib import Path
import pandas as pd

DATA_DIR   = Path("data")
PROJ_DIR   = DATA_DIR / "_projections"
RAW_DIR    = DATA_DIR / "raw"
SUMM_DIR   = Path("summaries") / "projections"
OUT_FILE   = PROJ_DIR / "pitcher_props_projected.csv"

TODAY_GAMES_FILE = PROJ_DIR / "todaysgames_normalized_fixed.csv"
ENRICHED_FILE    = RAW_DIR / "startingpitchers_with_opp_context.csv"

def _require(df: pd.DataFrame, cols: list, name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{name} missing required columns: {missing}")

def _to_str(df: pd.DataFrame, cols: list):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def load_todays_starters() -> pd.DataFrame:
    if not TODAY_GAMES_FILE.exists():
        raise FileNotFoundError(f"Missing input: {TODAY_GAMES_FILE}")
    g = pd.read_csv(TODAY_GAMES_FILE)
    g.columns = [c.strip() for c in g.columns]
    _require(g, ["home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"], "todaysgames")
    g = _to_str(g, ["home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"])

    # Flatten to one row per starter
    home = g.rename(columns={
        "pitcher_home_id": "player_id",
        "home_team_id": "team_id",
        "away_team_id": "opponent_team_id",
    })[["player_id", "team_id", "opponent_team_id"]].copy()
    home["home_away"] = "home"

    away = g.rename(columns={
        "pitcher_away_id": "player_id",
        "away_team_id": "team_id",
        "home_team_id": "opponent_team_id",
    })[["player_id", "team_id", "opponent_team_id"]].copy()
    away["home_away"] = "away"

    starters = pd.concat([home, away], ignore_index=True)
    # Some slates list the same pitcher twice; keep first
    starters = starters.drop_duplicates(subset=["player_id"], keep="first")
    return starters

def load_enriched() -> pd.DataFrame:
    if not ENRICHED_FILE.exists():
        raise FileNotFoundError(f"Missing input: {ENRICHED_FILE}")
    e = pd.read_csv(ENRICHED_FILE)
    e.columns = [c.strip() for c in e.columns]
    # Accept either 'player_id' or legacy 'playerid'
    if "player_id" not in e.columns and "playerid" in e.columns:
        e = e.rename(columns={"playerid": "player_id"})
    _require(e, ["player_id"], "startingpitchers_with_opp_context")
    e = _to_str(e, ["player_id", "team_id", "opponent_team_id"])
    return e

def main():
    PROJ_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    SUMM_DIR.mkdir(parents=True, exist_ok=True)

    starters = load_todays_starters()
    enriched = load_enriched()

    # --- Guard: make right side one-row-per player_id (fixes your error) ---
    dup_mask = enriched.duplicated(subset=["player_id"], keep="first")
    if dup_mask.any():
        # log what we are dropping for transparency
        enriched.loc[dup_mask].to_csv(SUMM_DIR / "pitcher_props_right_dupes.csv", index=False)
        enriched = enriched.drop_duplicates(subset=["player_id"], keep="first")

    # Merge many-to-one: left starters -> right enriched context
    merged = starters.merge(enriched, on="player_id", how="left", validate="one_to_one")

    # Keep all columns (downstream cleaners/post-normalizers will prune)
    merged.to_csv(OUT_FILE, index=False)
    print(f"Wrote: {OUT_FILE} (rows={len(merged)})  source=enriched")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
