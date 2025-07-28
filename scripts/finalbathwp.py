import pandas as pd
import os
import subprocess

def final_bat_hwp():
    """
    Processes baseball data by merging various input files to create a final
    batting average with runners in scoring scoring position (HWP) dataset for home batters.

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
        # Starting DataFrame for the merges
        final_df = pd.read_csv(bat_awp_clean2_path)
        batters_home_weather_df = pd.read_csv(batters_home_weather_path)
        batters_home_adjusted_df = pd.read_csv(batters_home_adjusted_path)
    except FileNotFoundError as e:
        print(f"❌ Error: Missing input file. Ensure all files are in the correct directory.")
        print(f"Missing file: {e.filename}")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # Merge 1: games_today_cleaned.csv for away_team, pitcher_away, pitcher_home
    # Use `home_team` from final_df to match in games_today_df
    games_cols = ['home_team', 'away_team', 'pitcher_away', 'pitcher_home']
    final_df = pd.merge(
        final_df,
        games_today_df[games_cols],
        on='home_team',
        how='left',
        suffixes=('', '_games') # Avoid column name clashes
    )

    # Merge 2: weather_adjustments.csv for venue, temperature, wind_speed, etc.
    # Use `home_team` from final_df to match in weather_adjustments_df
    weather_adj_cols = [
        'home_team', 'venue', 'temperature', 'wind_speed', 'wind_direction',
        'humidity', 'condition', 'game_time'
    ]
    final_df = pd.merge(
        final_df,
        weather_adjustments_df[weather_adj_cols],
        on='home_team',
        how='left',
        suffixes=('', '_adj') # Avoid column name clashes
    )

    # Merge 3: weather_input.csv for city, state, timezone, Park Factor, etc.
    # 'lat' and 'lon' are intentionally excluded here based on previous instructions.
    weather_input_cols = [
        'home_team', 'city', 'state', 'timezone', 'Park Factor', 'is_dome',
        'time_of_day'
    ]
    final_df = pd.merge(
        final_df,
        weather_input_df[weather_input_cols],
        on='home_team',
        how='left',
        suffixes=('', '_input') # Avoid column name clashes
    )

    # Merge 4: batters_home_weather.csv for adj_woba_weather
    if 'last_name' in final_df.columns and 'first_name' in final_df.columns:
        final_df['full_name_merge'] = final_df['first_name'].astype(str) + ' ' + final_df['last_name'].astype(str)
        batters_home_weather_df['name_lower'] = batters_home_weather_df['name'].astype(str).str.lower()
        final_df['full_name_merge_lower'] = final_df['full_name_merge'].astype(str).str.lower()

        final_df = pd.merge(
            final_df,
            batters_home_weather_df[['name_lower', 'adj_woba_weather']],
            left_on='full_name_merge_lower',
            right_on='name_lower',
            how='left'
        )
        final_df.drop(['full_name_merge', 'full_name_merge_lower', 'name_lower'], axis=1, inplace=True, errors='ignore')
    else:
        print("⚠️ Warning: 'last_name' or 'first_name' not found in bat_awp_clean2.csv. Skipping adj_woba_weather merge.")


    # Merge 5: batters_home_adjusted.csv for adj_woba_combined
    if 'last_name' in final_df.columns and 'first_name' in final_df.columns:
        final_df['full_name_merge'] = final_df['first_name'].astype(str) + ' ' + final_df['last_name'].astype(str)
        batters_home_adjusted_df['name_lower'] = batters_home_adjusted_df['name'].astype(str).str.lower()
        final_df['full_name_merge_lower'] = final_df['full_name_merge'].astype(str).str.lower()

        final_df = pd.merge(
            final_df,
            batters_home_adjusted_df[['name_lower', 'adj_woba_combined']],
            left_on='full_name_merge_lower',
            right_on='name_lower',
            how='left'
        )
        # Clean up temporary merge columns
        final_df.drop(['full_name_merge', 'full_name_merge_lower', 'name_lower'], axis=1, inplace=True, errors='ignore')
    else:
        print("⚠️ Warning: 'last_name' or 'first_name' not found in bat_awp_clean2.csv. Skipping adj_woba_combined merge.")

    # --- Renaming 'lat' to 'latitude' and 'lon' to 'longitude' in the final output ---
    rename_mapping = {}
    if 'lat' in final_df.columns:
        rename_mapping['lat'] = 'latitude'
    if 'lon' in final_df.columns:
        rename_mapping['lon'] = 'longitude'

    if rename_mapping: # Only call rename if there's something to rename
        final_df.rename(columns=rename_mapping, inplace=True)
        print(f"✅ Renamed columns in final output: {rename_mapping}")
    else:
        print("⚠️ Warning: 'lat' or 'lon' columns not found in the final DataFrame for renaming. Ensure they are present from an upstream process if desired in the output.")

    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Save the final DataFrame to a CSV file
    final_df.to_csv(output_filepath, index=False)
    print(f"✅ Successfully created '{output_filepath}'")

    # Git commit and push (assuming this script is run within a Git repo context)
    try:
        subprocess.run(["git", "add", output_filepath], check=True)
        commit_message = f"📊 Auto-generate {output_filename}"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed for {output_filename}: {e}")
        if e.stderr:
            print("Git stderr:", e.stderr.decode())
        if e.stdout:
            print("Git stdout:", e.stdout.decode())
    except FileNotFoundError:
        print("❌ Git command not found. Ensure Git is installed and in PATH.")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Git operations: {e}")

if __name__ == "__main__":
    final_bat_hwp()
