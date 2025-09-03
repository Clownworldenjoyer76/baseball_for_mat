#!/usr/bin/env python3
import os
import subprocess
import pandas as pd

# -------------------------------
# Paths (RELATIVE; no leading "/")
# -------------------------------
AWR_FILE      = "data/end_chain/first/raw/bat_awp_dirty.csv"   # must have: player_id, game_id
BATTERS_FILE  = "data/Data/batters.csv"                        # must have: player_id
GAMES_FILE    = "data/raw/todaysgames_normalized.csv"          # must have: game_id
WEATHER_FILE  = "data/weather_adjustments.csv"                 # must have: game_id
OUT_DIR       = "data/end_chain/final"
OUT_FILE      = os.path.join(OUT_DIR, "finalbatawp.csv")

def read_csv_safe(path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        print(f"❌ Missing input file: {path}")
        return None
    except Exception as e:
        print(f"❌ Error loading {path}: {e}")
        return None

def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def git_add_commit_push(filepath, message):
    try:
        subprocess.run(["git", "add", filepath], check=True)
        # Commit only if there is something to commit
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=True)
        if status.stdout.strip():
            subprocess.run(["git", "commit", "-m", message], check=True)
            subprocess.run(["git", "push"], check=True)
            print("✅ Pushed to repository.")
        else:
            print("ℹ️ No changes to commit.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")

def main():
    # Load inputs (assumption-free beyond keys)
    awr = read_csv_safe(AWR_FILE)
    bat = read_csv_safe(BATTERS_FILE)
    games = read_csv_safe(GAMES_FILE)
    weather = read_csv_safe(WEATHER_FILE)

    # Hard-stop validations (keys only)
    if awr is None or bat is None:
        return
    for req_col in ["player_id", "game_id"]:
        if req_col not in awr.columns:
            print(f"❌ {AWR_FILE} missing required column: {req_col}")
            return
    if "player_id" not in bat.columns:
        print(f"❌ {BATTERS_FILE} missing required column: player_id")
        return
    if games is not None and "game_id" not in games.columns:
        print(f"❌ {GAMES_FILE} missing required column: game_id")
        games = None
    if weather is not None and "game_id" not in weather.columns:
        print(f"❌ {WEATHER_FILE} missing required column: game_id")
        weather = None

    # Normalize ID types
    awr = coerce_numeric(awr, ["player_id", "game_id"])
    bat = coerce_numeric(bat, ["player_id"])
    if games is not None:
        games = coerce_numeric(games, ["game_id"])
    if weather is not None:
        weather = coerce_numeric(weather, ["game_id"])

    # Merge 1: player metrics on player_id (left join)
    merged = pd.merge(
        awr,
        bat,
        on="player_id",
        how="left",
        suffixes=("", "_bat")
    )

    # Merge 2: team/game context on game_id (left join)
    if games is not None:
        game_cols = ["game_id"] + [c for c in games.columns if c != "game_id"]
        merged = pd.merge(
            merged,
            games[game_cols].drop_duplicates(subset=["game_id"]),
            on="game_id",
            how="left",
            suffixes=("", "_game")
        )

    # Merge 3: weather on game_id (left join)
    if weather is not None:
        wx_cols = ["game_id"] + [c for c in weather.columns if c != "game_id"]
        overlap = set(wx_cols[1:]).intersection(set(merged.columns))
        wx = weather[wx_cols].copy()
        if overlap:
            wx = wx.rename(columns={c: f"{c}_wx" for c in overlap})
        merged = pd.merge(
            merged,
            wx,
            on="game_id",
            how="left",
            suffixes=("", "_wx")
        )

    # Deduplicate on (player_id, game_id)
    before = len(merged)
    merged = merged.sort_values(["player_id", "game_id"]).drop_duplicates(subset=["player_id", "game_id"], keep="first")
    removed = before - len(merged)
    if removed > 0:
        print(f"ℹ️ Removed {removed} duplicate rows on (player_id, game_id).")

    # Write output
    os.makedirs(OUT_DIR, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)
    print(f"✅ Created {OUT_FILE} (rows={len(merged)})")

    # Git
    git_add_commit_push(OUT_FILE, "📊 Build finalbatawp.csv from dirty AWP; player_id & game_id merges (relative paths)")

if __name__ == "__main__":
    main()
