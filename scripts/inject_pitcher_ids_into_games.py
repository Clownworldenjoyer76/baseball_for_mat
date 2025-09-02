# scripts/inject_pitcher_ids_into_games.py

import pandas as pd
from pathlib import Path
import sys

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
PLAYER_MASTER_FILE = Path("data/processed/player_team_master.csv")
REQUIRED_GAME_COLS = ["pitcher_home", "pitcher_away"]
REQUIRED_MASTER_COLS = ["player_id", "last_name, first_name"]

def load_games(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {path} not found")
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_GAME_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {path} missing columns: {missing}")
    # strip only; no renaming/normalization
    for c in REQUIRED_GAME_COLS:
        df[c] = df[c].astype(str).str.strip()
    return df

def load_master(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {path} not found")
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_MASTER_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {path} missing columns: {missing}")
    # strict: keep only exact id + "last_name, first_name"
    out = df[["player_id", "last_name, first_name"]].copy()
    out["last_name, first_name"] = out["last_name, first_name"].astype(str).str.strip()
    # ensure ids are integers
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["player_id", "last_name, first_name"])
    return out

def main():
    games = load_games(GAMES_FILE)
    master = load_master(PLAYER_MASTER_FILE)

    # build exact name→id map
    name_to_id = dict(zip(master["last_name, first_name"], master["player_id"]))

    # inject ids by exact match; no assumptions or fuzzy matching
    games["pitcher_home_id"] = games["pitcher_home"].map(name_to_id).astype("Int64")
    games["pitcher_away_id"] = games["pitcher_away"].map(name_to_id).astype("Int64")

    # write back, preserving all existing columns
    GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_FILE, index=False)
    print(f"✅ Injected IDs into {GAMES_FILE} "
          f"(home_id non-null: {games['pitcher_home_id'].notna().sum()}, "
          f"away_id non-null: {games['pitcher_away_id'].notna().sum()})")

if __name__ == "__main__":
    # no args; paths are fixed
    try:
        main()
    except Exception as e:
        print(str(e))
        sys.exit(1)
