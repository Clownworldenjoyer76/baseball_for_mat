# scripts/finalbathwp.py

import os
import subprocess
import pandas as pd

BATTERS_PATH = "data/end_chain/first/raw/bat_hwp_dirty.csv"
GAMES_PATH   = "data/end_chain/cleaned/games_today_cleaned.csv"
OUT_DIR      = "data/end_chain/final"
OUT_FILE     = os.path.join(OUT_DIR, "finalbathwp.csv")

REQ_BAT_COLS   = {"player_id", "game_id"}
REQ_GAME_COLS  = {"game_id"}  # home/away/game_time added if present

KEEP_GAME_COLS = [
    # always keep game_id from left side; these are appended if present
    "home_team", "away_team", "game_time", "pitcher_home", "pitcher_away"
]

def main():
    # Load inputs
    if not os.path.exists(BATTERS_PATH):
        raise SystemExit(f"❌ Missing batters file: {BATTERS_PATH}")
    if not os.path.exists(GAMES_PATH):
        raise SystemExit(f"❌ Missing games file: {GAMES_PATH}")

    bat = pd.read_csv(BATTERS_PATH)
    games = pd.read_csv(GAMES_PATH)

    # Validate required columns
    missing_bat = REQ_BAT_COLS - set(bat.columns)
    if missing_bat:
        raise SystemExit(f"❌ {BATTERS_PATH} missing required columns: {sorted(missing_bat)}")
    missing_game = REQ_GAME_COLS - set(games.columns)
    if missing_game:
        raise SystemExit(f"❌ {GAMES_PATH} missing required columns: {sorted(missing_game)}")

    # Normalize merge key dtypes: merge STRICTLY on game_id
    bat["game_id"] = bat["game_id"].astype("string")
    games["game_id"] = games["game_id"].astype("string")

    # Select only the game columns we care about (if they exist)
    game_cols = ["game_id"] + [c for c in KEEP_GAME_COLS if c in games.columns]
    games_small = games[game_cols].drop_duplicates()

    # Merge
    merged = bat.merge(games_small, on="game_id", how="left")

    # Write
    os.makedirs(OUT_DIR, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)
    print(f"✅ Built {OUT_FILE} (rows={len(merged)}, cols={len(merged.columns)})")

    # Commit
    try:
        subprocess.run(["git", "add", OUT_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "finalbathwp: game_id merge of bat_hwp_dirty with games"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed finalbathwp.csv")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push skipped or failed: {e}")

if __name__ == "__main__":
    main()
