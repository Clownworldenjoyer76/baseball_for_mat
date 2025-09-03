import pandas as pd
import os
import subprocess
import shutil

def prep_merge():
    """
    Prepare merge staging files (player_id/game_id based).

    Inputs:
      - data/adjusted/batters_home_weather_park.csv
      - data/adjusted/batters_away_weather_park.csv
      - data/adjusted/pitchers_home_weather_park.csv
      - data/adjusted/pitchers_away_weather_park.csv
      - data/raw/todaysgames_normalized.csv

    Outputs:
      - data/end_chain/first/pit_hwp.csv
      - data/end_chain/first/pit_awp.csv
      - data/end_chain/first/raw/bat_awp_dirty.csv
      - data/end_chain/first/raw/bat_hwp_dirty.csv
      - data/end_chain/first/games_today.csv

    Notes:
      - No name reformatting. Pipeline should join on player_id/game_id.
      - Batters (away) have *_y columns dropped and *_x suffixes stripped.
    """

    # Paths
    input_pitchers_home = 'data/adjusted/pitchers_home_weather_park.csv'
    input_pitchers_away = 'data/adjusted/pitchers_away_weather_park.csv'
    input_batters_home = 'data/adjusted/batters_home_weather_park.csv'
    input_batters_away = 'data/adjusted/batters_away_weather_park.csv'
    input_games_today  = 'data/raw/todaysgames_normalized.csv'

    output_pit_hwp       = 'data/end_chain/first/pit_hwp.csv'
    output_pit_awp       = 'data/end_chain/first/pit_awp.csv'
    output_bat_awp_dirty = 'data/end_chain/first/raw/bat_awp_dirty.csv'
    output_bat_hwp_dirty = 'data/end_chain/first/raw/bat_hwp_dirty.csv'
    output_games_today   = 'data/end_chain/first/games_today.csv'

    # Ensure output dirs
    os.makedirs(os.path.dirname(output_pit_hwp), exist_ok=True)
    os.makedirs(os.path.dirname(output_bat_awp_dirty), exist_ok=True)
    os.makedirs(os.path.dirname(output_games_today), exist_ok=True)

    # --- Pitchers: pass-through (no name edits) ---
    if os.path.exists(input_pitchers_home):
        df_pit_home = pd.read_csv(input_pitchers_home)
        df_pit_home.to_csv(output_pit_hwp, index=False)
        print(f"Saved pitchers home → {output_pit_hwp}")
    else:
        print(f"Warning: {input_pitchers_home} not found. Skipping.")

    if os.path.exists(input_pitchers_away):
        df_pit_away = pd.read_csv(input_pitchers_away)
        df_pit_away.to_csv(output_pit_awp, index=False)
        print(f"Saved pitchers away → {output_pit_awp}")
    else:
        print(f"Warning: {input_pitchers_away} not found. Skipping.")

    # --- Batters: clean away; copy home ---
    if os.path.exists(input_batters_away):
        df_bat_away = pd.read_csv(input_batters_away)
        cols_to_drop = [c for c in df_bat_away.columns if c.endswith('_y')]
        if cols_to_drop:
            df_bat_away.drop(columns=cols_to_drop, inplace=True)
        df_bat_away.columns = [c.replace('_x', '') for c in df_bat_away.columns]
        df_bat_away.to_csv(output_bat_awp_dirty, index=False)
        print(f"Saved cleaned batters away → {output_bat_awp_dirty}")
    else:
        print(f"Warning: {input_batters_away} not found. Skipping.")

    if os.path.exists(input_batters_home):
        df_bat_home = pd.read_csv(input_batters_home)
        df_bat_home.to_csv(output_bat_hwp_dirty, index=False)
        print(f"Saved batters home → {output_bat_hwp_dirty}")
    else:
        print(f"Warning: {input_batters_home} not found. Skipping.")

    # --- Games: copy to staging ---
    if os.path.exists(input_games_today):
        shutil.copy2(input_games_today, output_games_today)
        print(f"Copied games file → {output_games_today}")
    else:
        print(f"Warning: {input_games_today} not found. Skipping.")

    # Commit staged outputs
    commit_and_push([
        output_pit_hwp,
        output_pit_awp,
        output_bat_awp_dirty,
        output_bat_hwp_dirty,
        output_games_today
    ])

def commit_and_push(paths):
    try:
        # Only add paths that currently exist
        paths = [p for p in paths if os.path.exists(p)]
        if not paths:
            print("Nothing to commit.")
            return
        subprocess.run(["git", "add"] + paths, check=True)
        subprocess.run(["git", "commit", "-m", "prep_merge: stage files (player_id/game_id-based)"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commit/push complete.")
    except subprocess.CalledProcessError as e:
        print(f"Git operation failed: {e}")

if __name__ == "__main__":
    prep_merge()
