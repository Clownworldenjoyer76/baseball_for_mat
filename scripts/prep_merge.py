import pandas as pd
import os
import subprocess
import shutil

def prep_merge():
    """
    Prepares baseball data files for merging.

    This script performs the following operations:
    1. Updates 'last_name, first_name' column format in pitchers' data files
       from "First, Last" to "Last, First".
    2. Cleans column headers in the batters_away_weather_park.csv file
       by removing '_x' suffixes and deleting columns with '_y' suffixes.
    3. Copies batters_home_weather_park.csv to a new raw directory.

    Input files:
    - data/adjusted/batters_home_weather_park.csv
    - data/adjusted/batters_away_weather_park.csv
    - data/adjusted/pitchers_home_weather_park.csv
    - data/adjusted/pitchers_away_weather_park.csv

    Output files:
    - data/end_chain/first/pit_hwp.csv
    - data/end_chain/first/pit_awp.csv
    - data/end_chain/first/raw/bat_awp_dirty.csv
    - data/end_chain/first/raw/bat_hwp_dirty.csv
    """

    # Define input and output file paths
    input_pitchers_home = 'data/adjusted/pitchers_home_weather_park.csv'
    input_pitchers_away = 'data/adjusted/pitchers_away_weather_park.csv'
    input_batters_home = 'data/adjusted/batters_home_weather_park.csv'
    input_batters_away = 'data/adjusted/batters_away_weather_park.csv'
    input_games_today = 'data/raw/todaysgames_normalized.csv'

    output_pit_hwp = 'data/end_chain/first/pit_hwp.csv'
    output_pit_awp = 'data/end_chain/first/pit_awp.csv'
    output_bat_awp_dirty = 'data/end_chain/first/raw/bat_awp_dirty.csv'
    output_bat_hwp_dirty = 'data/end_chain/first/raw/bat_hwp_dirty.csv'
    output_games_today = 'data/end_chain/games_today.csv'

    # Ensure output directories exist
    os.makedirs(os.path.dirname(output_pit_hwp), exist_ok=True)
    os.makedirs(os.path.dirname(output_bat_awp_dirty), exist_ok=True)
    os.makedirs(os.path.dirname(output_games_today), exist_ok=True)

    # Function to update name format
    def update_name_format(df, column_name):
        if column_name in df.columns:
            df[column_name] = df[column_name].astype(str).str.strip().str.replace(r',\s*$', '', regex=True)
            df[column_name] = df[column_name].apply(
                lambda x: f"{x.split(', ')[1]}, {x.split(', ')[0]}" if ', ' in x else x
            )
        return df

    # Process pitchers_home_weather_park.csv
    if os.path.exists(input_pitchers_home):
        print(f"Processing {input_pitchers_home}...")
        df_pit_home = pd.read_csv(input_pitchers_home)
        df_pit_home = update_name_format(df_pit_home, 'last_name, first_name')
        df_pit_home.to_csv(output_pit_hwp, index=False)
        print(f"Saved updated pitchers home data to {output_pit_hwp}")
    else:
        print(f"Warning: {input_pitchers_home} not found. Skipping.")

    # Process pitchers_away_weather_park.csv
    if os.path.exists(input_pitchers_away):
        print(f"Processing {input_pitchers_away}...")
        df_pit_away = pd.read_csv(input_pitchers_away)
        df_pit_away = update_name_format(df_pit_away, 'last_name, first_name')
        df_pit_away.to_csv(output_pit_awp, index=False)
        print(f"Saved updated pitchers away data to {output_pit_awp}")
    else:
        print(f"Warning: {input_pitchers_away} not found. Skipping.")

    # Process batters_away_weather_park.csv
    if os.path.exists(input_batters_away):
        print(f"Processing {input_batters_away}...")
        df_bat_away = pd.read_csv(input_batters_away)
        cols_to_drop = [col for col in df_bat_away.columns if col.endswith('_y')]
        df_bat_away.drop(columns=cols_to_drop, inplace=True)
        df_bat_away.columns = [col.replace('_x', '') for col in df_bat_away.columns]
        df_bat_away.to_csv(output_bat_awp_dirty, index=False)
        print(f"Saved cleaned batters away data to {output_bat_awp_dirty}")
    else:
        print(f"Warning: {input_batters_away} not found. Skipping.")

    # Copy batters_home_weather_park.csv
    if os.path.exists(input_batters_home):
        print(f"Copying {input_batters_home}...")
        df_bat_home = pd.read_csv(input_batters_home)
        df_bat_home.to_csv(output_bat_hwp_dirty, index=False)
        print(f"Copied batters home data to {output_bat_hwp_dirty}")
    else:
        print(f"Warning: {input_batters_home} not found. Skipping.")

    # Copy todaysgames_normalized.csv to games_today.csv
    if os.path.exists(input_games_today):
        shutil.copy2(input_games_today, output_games_today)
        print(f"Copied {input_games_today} to {output_games_today}")
    else:
        print(f"Warning: {input_games_today} not found. Skipping.")

    # Git commit and push
    commit_and_push([
        output_pit_hwp,
        output_pit_awp,
        output_bat_awp_dirty,
        output_bat_hwp_dirty,
        output_games_today
    ])

def commit_and_push(paths):
    try:
        subprocess.run(["git", "add"] + paths, check=True)
        subprocess.run(["git", "commit", "-m", "prep_merge: cleaned data and copied games_today"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git operation failed: {e}")

if __name__ == "__main__":
    prep_merge()
