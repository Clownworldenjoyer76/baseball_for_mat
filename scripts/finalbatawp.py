#!/usr/bin/env python3
import os
import subprocess
import pandas as pd

GAMES_FILE = "/data/end_chain/cleaned/games_today_cleaned.csv"           # context (game_id, teams, game_time, pitchers)
GAMES_ALT  = "/data/raw/todaysgames_normalized.csv"                      # fallback/extra context if needed
BATTERS_FILE = "/data/Data/batters.csv"                                  # all batter metrics keyed by player_id
AWR_FILE  = "/data/end_chain/first/raw/bat_awp_dirty.csv"                # player_id, game_id, adj_woba_*
WEATHER_FILE = "/data/weather_adjustments.csv"                            # weather keyed by game_id
OUT_DIR   = "/data/end_chain/final"
OUT_FILE  = os.path.join(OUT_DIR, "finalbatawp.csv")

def _read_csv(path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        print(f"❌ Missing input file: {path}")
        return None
    except Exception as e:
        print(f"❌ Error loading {path}: {e}")
        return None

def _coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def main():
    # Load inputs
    awr = _read_csv(AWR_FILE)
    bat = _read_csv(BATTERS_FILE)
    games = _read_csv(GAMES_FILE)
    games_alt = _read_csv(GAMES_ALT)
    weather = _read_csv(WEATHER_FILE)

    if awr is None or bat is None:
        return

    # Validate minimal required columns
    req_awr = {"player_id", "game_id", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"}
    missing_awr = req_awr - set(awr.columns)
    if missing_awr:
        print(f"❌ {AWR_FILE} missing columns: {sorted(missing_awr)}")
        return

    if "player_id" not in bat.columns:
        print(f"❌ {BATTERS_FILE} missing 'player_id'")
        return

    # Normalize ids
    awr = _coerce_numeric(awr, ["player_id", "game_id"])
    bat = _coerce_numeric(bat, ["player_id"])

    # --- Merge 1: player-level batter metrics ---
    # Keep all batter columns; AWR is the driver (left merge)
    merged = pd.merge(
        awr,
        bat,
        on="player_id",
        how="left",
        suffixes=("", "_bat")
    )

    # --- Merge 2: game/team context on game_id ---
    # Prefer cleaned games; if not available, fall back to normalized.
    game_ctx = None
    if games is not None and "game_id" in games.columns:
        game_ctx = games.copy()
    elif games_alt is not None and "game_id" in games_alt.columns:
        game_ctx = games_alt.copy()

    if game_ctx is not None:
        # Select common context columns if present
        wanted_game_cols = [
            "game_id", "home_team", "away_team", "game_time",
            "pitcher_home", "pitcher_away", "stadium", "location", "Park Factor", "time_of_day"
        ]
        have_game_cols = [c for c in wanted_game_cols if c in game_ctx.columns]
        game_ctx = game_ctx[have_game_cols].drop_duplicates()

        merged = pd.merge(
            merged,
            game_ctx,
            on="game_id",
            how="left",
            suffixes=("", "_game")
        )

    # --- Merge 3: weather on game_id ---
    if weather is not None and "game_id" in weather.columns:
        # Pass through all weather columns except those already present to avoid clobber
        weather_cols = [c for c in weather.columns if c != "game_id"]
        weather_use = ["game_id"] + weather_cols
        merged = pd.merge(
            merged,
            weather[weather_use],
            on="game_id",
            how="left",
            suffixes=("", "_wx")
        )

    # Deduplicate by (player_id, game_id) if accidental one-to-many joins occurred
    # Keep the first occurrence deterministically
    before = len(merged)
    merged = merged.sort_values(["player_id", "game_id"]).drop_duplicates(subset=["player_id", "game_id"], keep="first")
    after = len(merged)
    if after < before:
        print(f"ℹ️ Removed {before - after} duplicate rows on (player_id, game_id).")

    # Output
    os.makedirs(OUT_DIR, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)
    print(f"✅ Created {OUT_FILE} (rows={len(merged)})")

    # Git commit/push
    try:
        subprocess.run(["git", "add", OUT_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "📊 Build finalbatawp.csv from dirty AWP; merge on player_id + game_id context"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")

if __name__ == "__main__":
    main()
