#!/usr/bin/env python3
import pandas as pd
import os
import subprocess

def final_bat_awp():
    """
    Build final batting-away (AWP) dataset.

    Input:
      - data/end_chain/cleaned/games_today_cleaned.csv
      - data/end_chain/cleaned/bat_awp_cleaned.csv

    Output:
      - data/end_chain/final/finalbatawp.csv
    """

    games_file = "data/end_chain/cleaned/games_today_cleaned.csv"
    bat_file = "data/end_chain/cleaned/bat_awp_cleaned.csv"
    output_dir = "data/end_chain/final"
    output_file = os.path.join(output_dir, "finalbatawp.csv")

    # Load inputs
    try:
        games = pd.read_csv(games_file)
        batters = pd.read_csv(bat_file)
    except FileNotFoundError as e:
        print(f"❌ Missing input file: {e.filename}")
        return
    except Exception as e:
        print(f"❌ Error loading input: {e}")
        return

    # Ensure merge keys exist
    needed_cols = {"away_team", "home_team"}
    if not needed_cols.issubset(games.columns):
        print(f"❌ games_today_cleaned.csv missing columns: {needed_cols - set(games.columns)}")
        return
    if "away_team" not in batters.columns:
        print("❌ bat_awp_cleaned.csv missing 'away_team' column.")
        return

    # Defensive type normalization
    for col in ["away_team", "home_team"]:
        if col in games.columns:
            games[col] = games[col].astype(str).str.strip()
        if col in batters.columns:
            batters[col] = batters[col].astype(str).str.strip()

    # Merge on both away_team and home_team to attach game_id
    merged = pd.merge(
        batters,
        games[["game_id", "away_team", "home_team", "game_time"]],
        on=["away_team", "home_team"],
        how="left"
    )

    if merged["game_id"].isna().any():
        missing = merged[merged["game_id"].isna()][["away_team", "home_team"]]
        print("⚠️ Warning: Some rows missing game_id after merge:")
        print(missing.drop_duplicates().to_string(index=False))

    # Output
    os.makedirs(output_dir, exist_ok=True)
    merged.to_csv(output_file, index=False)
    print(f"✅ Successfully created {output_file} (rows={len(merged)})")

    # Git commit
    try:
        subprocess.run(["git", "add", output_file], check=True)
        subprocess.run(["git", "commit", "-m", "📊 Auto-update finalbatawp.csv"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")

if __name__ == "__main__":
    final_bat_awp()
