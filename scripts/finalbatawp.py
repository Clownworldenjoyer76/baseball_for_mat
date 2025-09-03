# scripts/finalbatawp.py

import os
import subprocess
import pandas as pd

GAMES_PATH = "data/end_chain/cleaned/games_today_cleaned.csv"
BAT_AWP_PATH = "data/end_chain/cleaned/bat_awp_cleaned.csv"

OUTPUT_DIR = "data/end_chain/final"
OUTPUT_FILE = "finalbatawp.csv"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILE)


def _pick_game_time_col(df: pd.DataFrame) -> str | None:
    """Return the name of the game time column if present (game_time or game_time_et)."""
    if "game_time" in df.columns:
        return "game_time"
    if "game_time_et" in df.columns:
        return "game_time_et"
    return None


def final_bat_awp():
    """
    Build finalbatawp.csv by merging batting AWP data with today's games metadata.
    - No weather merges.
    - Prefer merge on game_id. If bat_awp_cleaned.csv lacks game_id, infer it from away_team.
    """
    # --- Load inputs ---
    try:
        games_df = pd.read_csv(GAMES_PATH)
        bat_df = pd.read_csv(BAT_AWP_PATH)
    except FileNotFoundError as e:
        print("❌ Error: Missing input file.")
        print(f"   Missing: {e.filename}")
        return
    except Exception as e:
        print(f"❌ Error loading inputs: {e}")
        return

    # --- Normalize minimal columns/types we need from games ---
    games_cols = ["game_id", "away_team", "home_team"]
    time_col = _pick_game_time_col(games_df)
    if time_col:
        games_cols.append(time_col)

    missing_games_cols = [c for c in ["game_id", "away_team"] if c not in games_df.columns]
    if missing_games_cols:
        print(f"❌ Error: games file is missing required columns: {missing_games_cols}")
        return

    games_slim = games_df[[c for c in games_cols if c in games_df.columns]].copy()
    # Deduplicate on game_id first; if no game_id yet, dedup on away_team for the day.
    if "game_id" in games_slim.columns:
        games_slim = games_slim.drop_duplicates(subset=["game_id"])
    else:
        games_slim = games_slim.drop_duplicates(subset=["away_team"])

    # --- Ensure bat_df has game_id; if not, backfill via away_team ---
    if "game_id" not in bat_df.columns:
        if "away_team" not in bat_df.columns:
            print("❌ Error: 'game_id' missing in bat_awp_cleaned.csv and no 'away_team' to infer from.")
            return
        # Bring in game_id (and home/time if useful) via away_team
        bring_cols = ["away_team", "game_id", "home_team"]
        if time_col: 
            bring_cols.append(time_col)
        bring_cols = [c for c in bring_cols if c in games_slim.columns]

        bat_df = bat_df.merge(games_slim[bring_cols], on="away_team", how="left", suffixes=("", "_games"))
        if "game_id" not in bat_df.columns or bat_df["game_id"].isnull().all():
            print("❌ Could not infer 'game_id' from away_team. Check games_today_cleaned.csv.")
            return
        print("ℹ️ 'game_id' was absent in bat_awp_cleaned.csv; inferred from away_team.")

    # --- Now do a clean merge keyed by game_id to guarantee correct home/time ---
    # Keep only unique rows per game in games_slim to avoid duplication.
    key_games_cols = ["game_id", "home_team"]
    if time_col:
        key_games_cols.append(time_col)
    games_key = games_slim[[c for c in key_games_cols if c in games_slim.columns]].drop_duplicates("game_id")

    final_df = bat_df.merge(games_key, on="game_id", how="left", suffixes=("", "_games"))

    # Standardize column names for output (prefer 'game_time' name if we merged 'game_time_et')
    if time_col and time_col in final_df.columns and time_col != "game_time":
        # If the file used game_time_et, expose it as 'game_time' in output for consistency
        final_df.rename(columns={time_col: "game_time"}, inplace=True)

    # --- Write output ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Successfully created '{OUTPUT_PATH}'")

    # --- Git commit/push (best-effort) ---
    try:
        subprocess.run(["git", "add", OUTPUT_PATH], check=True)
        subprocess.run(["git", "commit", "-m", f"📊 Auto-generate {OUTPUT_FILE} (game_id-anchored, no weather)"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git push failed for {OUTPUT_FILE}: {e}")
    except FileNotFoundError:
        print("⚠️ Git not found; skipping commit/push.")


if __name__ == "__main__":
    final_bat_awp()
