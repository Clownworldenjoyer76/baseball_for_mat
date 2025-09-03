#!/usr/bin/env python3
import os
import sys
import pandas as pd

BAT_AWP_CLEAN = "data/end_chain/cleaned/bat_awp_cleaned.csv"
GAMES_CLEAN   = "data/end_chain/cleaned/games_today_cleaned.csv"
OUT_DIR       = "data/end_chain/final"
OUT_FILE      = os.path.join(OUT_DIR, "finalbatawp.csv")

def ts(msg):  # tiny helper for readable logs
    print(msg, flush=True)

def ensure_cols(df, needed, name):
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"❌ {name} missing columns: {missing}")

def attach_game_id_if_needed(bat, games):
    """
    If batters file lacks game_id, attempt to attach it from games by
    matching (away_team, home_team) when both exist, otherwise by away_team only.
    """
    if "game_id" in bat.columns and bat["game_id"].notna().any():
        return bat  # already present

    # Prefer attaching with both away/home teams if available
    keys_both = {"away_team", "home_team"}
    keys_away = {"away_team"}

    games_small = games[["game_id", "away_team", "home_team", "game_time"]].drop_duplicates()

    if keys_both.issubset(bat.columns):
        merged = bat.merge(games_small[["game_id", "away_team", "home_team"]],
                           on=["away_team", "home_team"], how="left")
        if merged["game_id"].notna().any():
            ts("⚠️ bat_awp_cleaned.csv had no game_id; attached via (away_team, home_team).")
            return merged

    if keys_away.issubset(bat.columns):
        merged = bat.merge(games_small[["game_id", "away_team"]],
                           on=["away_team"], how="left")
        if merged["game_id"].notna().any():
            ts("⚠️ bat_awp_cleaned.csv had no game_id; attached via away_team.")
            return merged

    ts("⚠️ Unable to attach game_id from games; proceeding without games join.")
    return bat

def main():
    # Load inputs
    try:
        bat = pd.read_csv(BAT_AWP_CLEAN)
        games = pd.read_csv(GAMES_CLEAN)
    except FileNotFoundError as e:
        print(f"❌ Missing input file: {e.filename}")
        sys.exit(1)

    # Basic schema checks
    ensure_cols(games, ["game_id", "home_team", "away_team", "game_time"], "games_today_cleaned.csv")

    # Ensure/attach game_id in batters file if needed
    if "game_id" not in bat.columns or not bat["game_id"].notna().any():
        bat = attach_game_id_if_needed(bat, games)

    # If we still don't have game_id, we’ll save without the games join
    if "game_id" in bat.columns and bat["game_id"].notna().any():
        # Merge strictly on game_id, bringing over canonical matchup/time
        games_small = games[["game_id", "home_team", "away_team", "game_time"]].drop_duplicates()
        final_df = bat.merge(games_small, on="game_id", how="left")
    else:
        final_df = bat.copy()

    # Write output
    os.makedirs(OUT_DIR, exist_ok=True)
    final_df.to_csv(OUT_FILE, index=False)
    ts(f"✅ Successfully created '{OUT_FILE}' (rows={len(final_df)})")

if __name__ == "__main__":
    main()
