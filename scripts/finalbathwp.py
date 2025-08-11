import pandas as pd
import os
import subprocess

def _select_weather_adj_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select/rename columns from weather_adjustments.csv defensively.
    Handles 'game_time' vs 'game_time_et' and keeps only needed fields.
    """
    df = df.copy()
    cols_map = {
        "temperature": "temperature",
        "wind_speed": "wind_speed",
        "wind_direction": "wind_direction",
        "humidity": "humidity",
        "condition": "condition",
        "home_team": "home_team",
        # game_time may be 'game_time' or 'game_time_et'
    }
    # Pick game_time column
    if "game_time" in df.columns:
        cols_map["game_time"] = "game_time"
    elif "game_time_et" in df.columns:
        cols_map["game_time_et"] = "game_time"  # rename to game_time
    else:
        # proceed without it
        pass

    keep = [src for src in cols_map.keys() if src in df.columns]
    out = df[keep].rename(columns=cols_map)

    # Normalize types
    for c in ["home_team"]:
        if c in out.columns:
            out[c] = out[c].astype(str)
    if "temperature" in out.columns:
        out["temperature"] = pd.to_numeric(out["temperature"], errors="coerce").round(1)

    return out

def final_bat_hwp():
    """
    Processes baseball data by merging various input files to create a final
    batting average with runners in scoring position (HWP) dataset for home batters.

    Input files:
    - data/end_chain/cleaned/games_today_cleaned.csv
    - data/weather_adjustments.csv
    - data/weather_input.csv
    - data/end_chain/cleaned/bat_awp_clean2.csv
    - data/adjusted/batters_home_weather.csv
    - data/adjusted/batters_home_adjusted.csv

    Output file:
    - data/end_chain/final/finalbathwp.csv
    """
    # Define input and output file paths
    games_today_path = 'data/end_chain/cleaned/games_today_cleaned.csv'
    weather_adjustments_path = 'data/weather_adjustments.csv'
    weather_input_path = 'data/weather_input.csv'
    bat_awp_clean2_path = 'data/end_chain/cleaned/bat_awp_clean2.csv'
    batters_home_weather_path = 'data/adjusted/batters_home_weather.csv'
    batters_home_adjusted_path = 'data/adjusted/batters_home_adjusted.csv'

    output_directory = 'data/end_chain/final'
    output_filename = 'finalbathwp.csv'
    output_filepath = os.path.join(output_directory, output_filename)

    # Load input files
    try:
        games_today_df = pd.read_csv(games_today_path)
        weather_adjustments_df = pd.read_csv(weather_adjustments_path)
        weather_input_df = pd.read_csv(weather_input_path)
        final_df = pd.read_csv(bat_awp_clean2_path)  # starting frame
        batters_home_weather_df = pd.read_csv(batters_home_weather_path)
        batters_home_adjusted_df
