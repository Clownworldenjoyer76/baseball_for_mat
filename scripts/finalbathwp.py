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
        batters_home_adjusted_df = pd.read_csv(batters_home_adjusted_path)
    except FileNotFoundError as e:
        print(f"❌ Error: Missing input file. Ensure all files are in the correct directory.")
        print(f"Missing file: {e.filename}")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # Merge 1: games_today_cleaned.csv for away_team, pitcher_away, pitcher_home (by home_team)
    games_cols = [c for c in ['home_team', 'away_team', 'pitcher_away', 'pitcher_home'] if c in games_today_df.columns]
    final_df = pd.merge(
        final_df,
        games_today_df[games_cols],
        on='home_team',
        how='left',
        suffixes=('', '_games')
    )

    # Merge 2: weather_adjustments.csv (venue, temperature, wind, humidity, condition, game_time)
    # Handle game_time/game_time_et gracefully
    weather_adj_core = _select_weather_adj_cols(weather_adjustments_df)
    # Bring venue if present
    if 'venue' in weather_adjustments_df.columns and 'home_team' in weather_adjustments_df.columns:
        venue_df = weather_adjustments_df[['home_team', 'venue']].copy()
        weather_adj_core = pd.merge(weather_adj_core, venue_df, on='home_team', how='left')
    final_df = pd.merge(
        final_df,
        weather_adj_core,
        on='home_team',
        how='left',
        suffixes=('', '_adj')
    )

    # Merge 3: weather_input.csv (city, state, timezone, Park Factor, is_dome, time_of_day) by home_team
    weather_input_cols = [c for c in ['home_team', 'city', 'state', 'timezone', 'Park Factor', 'is_dome', 'time_of_day'] if c in weather_input_df.columns]
    final_df = pd.merge(
        final_df,
        weather_input_df[weather_input_cols],
        on='home_team',
        how='left',
        suffixes=('', '_input')
    )

    # Merge 4: batters_home_weather.csv for adj_woba_weather (by player name)
    player_name_col = "last_name, first_name"
    if player_name_col in final_df.columns and "name" in batters_home_weather_df.columns:
        final_df[f'{player_name_col}_lower'] = final_df[player_name_col].astype(str).str.lower()
        bhw = batters_home_weather_df.copy()
        bhw['name_lower'] = bhw['name'].astype(str).str.lower()
        final_df = pd.merge(
            final_df,
            bhw[['name_lower', 'adj_woba_weather']],
            left_on=f'{player_name_col}_lower',
            right_on='name_lower',
            how='left'
        )
        final_df.drop([f'{player_name_col}_lower', 'name_lower'], axis=1, inplace=True, errors='ignore')
    else:
        print(f"⚠️ Warning: Could not merge adj_woba_weather (missing '{player_name_col}' in final_df or 'name' in batters_home_weather.csv).")

    # Merge 5: batters_home_adjusted.csv for adj_woba_combined (by player name)
    if player_name_col in final_df.columns and "name" in batters_home_adjusted_df.columns:
        final_df[f'{player_name_col}_lower_adj'] = final_df[player_name_col].astype(str).str.lower()
        bha = batters_home_adjusted_df.copy()
        bha['name_lower_adj'] = bha['name'].astype(str).str.lower()
        final_df = pd.merge(
            final_df,
            bha[['name_lower_adj', 'adj_woba_combined']],
            left_on=f'{player_name_col}_lower_adj',
            right_on='name_lower_adj',
            how='left'
        )
        final_df.drop([f'{player_name_col}_lower_adj', 'name_lower_adj'], axis=1, inplace=True, errors='ignore')
    else:
        print(f"⚠️ Warning: Could not merge adj_woba_combined (missing '{player_name_col}' in final_df or 'name' in batters_home_adjusted.csv).")

    # Rename lat/lon if present
    rename_mapping = {}
    if 'lat' in final_df.columns: rename_mapping['lat'] = 'latitude'
    if 'lon' in final_df.columns: rename_mapping['lon'] = 'longitude'
    if rename_mapping:
        final_df.rename(columns=rename_mapping, inplace=True)
        print(f"✅ Renamed columns in final output: {rename_mapping}")
    else:
        print("ℹ️ Note: 'lat'/'lon' not found in final DataFrame (ok if not needed).")

    # Ensure output dir and write
    os.makedirs(output_directory, exist_ok=True)
    final_df.to_csv(output_filepath, index=False)
    print(f"✅ Successfully created '{output_filepath}'")

    # Git commit and push
    try:
        subprocess.run(["git", "add", output_filepath], check=True)
        commit_message = f"📊 Auto-generate {output_filename}"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed for {output_filename}: {e}")
        if getattr(e, "stderr", None):
            try:
                print("Git stderr:", e.stderr.decode())
            except Exception:
                pass
        if getattr(e, "stdout", None):
            try:
                print("Git stdout:", e.stdout.decode())
            except Exception:
                pass
    except FileNotFoundError:
        print("❌ Git command not found. Ensure Git is installed and in PATH.")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Git operations: {e}")

if __name__ == "__main__":
    final_bat_hwp()
