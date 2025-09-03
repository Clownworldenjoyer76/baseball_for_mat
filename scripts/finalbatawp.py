# scripts/finalbatawp.py

import os
import subprocess
import pandas as pd

BATTERS_PATH = "data/end_chain/first/raw/bat_awp_dirty.csv"
GAMES_PATH   = "data/end_chain/cleaned/games_today_cleaned.csv"
OUT_DIR      = "data/end_chain/final"
OUT_FILE     = os.path.join(OUT_DIR, "finalbatawp.csv")

REQ_BAT_COLS   = {"player_id", "game_id"}
REQ_GAME_COLS  = {"game_id"}  # home/away/game_time added if present

KEEP_GAME_COLS = [
    # always keep game_id from left side; these are appended if present
    "home_team", "away_team", "game_time", "pitcher_home", "pitcher_away"
]

def apply_park_defaults(df: pd.DataFrame) -> pd.DataFrame:
    # Treat empty strings as NA, then fill defaults
    if "park_factor_100" in df.columns:
        df["park_factor_100"] = (
            pd.to_numeric(df["park_factor_100"], errors="coerce")
              .fillna(100.0)
        )
    if "park_factor_src" in df.columns:
        df["park_factor_src"] = df["park_factor_src"].replace("", pd.NA).fillna("default")
    return df

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

    # Normalize merge key dtypes: merge STRICTLY on game_id (and keep player rows)
    bat["game_id"] = bat["game_id"].astype("string")
    games["game_id"] = games["game_id"].astype("string")

    # Select only the game columns we care about (if they exist)
    game_cols = ["game_id"] + [c for c in KEEP_GAME_COLS if c in games.columns]
    games_small = games[game_cols].drop_duplicates()

    # Merge (keep all bat rows)
    merged = bat.merge(games_small, on="game_id", how="left")

    # --- NEW: Default park factor fallback ---
    merged = apply_park_defaults(merged)

    # Write
    os.makedirs(OUT_DIR, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)
    print(f"📝 Wrote {OUT_FILE} (rows={len(merged)})")

    # Commit
    try:
        subprocess.run(["git", "add", OUT_FILE], check=True)
        subprocess.run(
            ["git", "commit", "-m", "📊 Build finalbatawp.csv; add default park_factor (100.0) + src=default fallback"],
            check=True
        )
        subprocess.run(["git", "push"], check=True)
        print("↗️ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git operation failed: {e}")

if __name__ == "__main__":
    main()
