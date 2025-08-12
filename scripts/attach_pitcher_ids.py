#!/usr/bin/env python3
import argparse
import pandas as pd
from pathlib import Path

GAMES_FILE_DEFAULT = Path("data/end_chain/cleaned/games_today_cleaned.csv")
STARTERS_FILE      = Path("data/end_chain/final/startingpitchers_final.csv")
OUT_GAMES_FILE     = Path("data/end_chain/cleaned/games_today_cleaned.csv")  # in-place update

def _key(s: str) -> str:
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", default=str(GAMES_FILE_DEFAULT))
    args = ap.parse_args()

    games_path = Path(args.games)
    if not games_path.exists():
        raise SystemExit(f"❌ Missing games file: {games_path}")

    g = pd.read_csv(games_path)
    if STARTERS_FILE.exists():
        sp = pd.read_csv(STARTERS_FILE)
    else:
        print("⚠️ No starters file; leaving pitcher IDs empty.")
        sp = pd.DataFrame(columns=["game_id","home_pitcher_id","away_pitcher_id"])

    # normalize keys
    for c in ("home_team","away_team"):
        if c in g.columns:
            g[c] = g[c].astype(str)
    g["home_key"] = g["home_team"].apply(_key) if "home_team" in g.columns else ""
    g["away_key"] = g["away_team"].apply(_key) if "away_team" in g.columns else ""

    # If starters already include game_id, join on that. Else, fall back on team keys.
    joined = g.copy()
    if "game_id" in g.columns and g["game_id"].notna().any() and "game_id" in sp.columns:
        sp_small = sp[["game_id","home_pitcher_id","away_pitcher_id"]].drop_duplicates()
        joined = joined.merge(sp_small, on="game_id", how="left")
    else:
        # attempt fallback match (same date/team matchups inside the starters file if present)
        if {"home_team","away_team"}.issubset(sp.columns):
            sp["home_key"] = sp["home_team"].apply(_key)
            sp["away_key"] = sp["away_team"].apply(_key)
            sp_small = sp[["home_key","away_key","home_pitcher_id","away_pitcher_id"]].drop_duplicates()
            joined = joined.merge(sp_small, on=["home_key","away_key"], how="left")
        else:
            # nothing to attach; create empty columns
            joined["home_pitcher_id"] = pd.NA
            joined["away_pitcher_id"] = pd.NA

    # write back
    OUT_GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    joined.to_csv(OUT_GAMES_FILE, index=False)
    print("✅ Attached pitcher IDs (where available) to games file.")

if __name__ == "__main__":
    main()
