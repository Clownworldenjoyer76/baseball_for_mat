import pandas as pd
import numpy as np

def project_prep():
    """
    Performs data preparation steps for baseball projection data.

    This script loads various CSV files, performs column creation, deletion,
    and data injection based on specified matching criteria.
    """

    # --- File Paths ---
    PATHS = {
        "bat_today_final": "data/end_chain/final/bat_today_final.csv",
        "batter_away_final": "data/end_chain/final/batter_away_final.csv",
        "batter_home_final": "data/end_chain/final/batter_home_final.csv",
        "startingpitchers_final": "data/end_chain/final/startingpitchers_final.csv",
        "pitchers_normalized_cleaned": "data/cleaned/pitchers_normalized_cleaned.csv",
        "weather_input": "data/weather_input.csv",
        "weather_adjustments": "data/weather_adjustments.csv",
        "stadium_metadata": "data/Data/stadium_metadata.csv",
        "final_scores_projected": "data/_projections/final_scores_projected.csv"
    }

    # --- Load DataFrames ---
    try:
        df_bat_today = pd.read_csv(PATHS["bat_today_final"])
        df_batter_away = pd.read_csv(PATHS["batter_away_final"])
        df_batter_home = pd.read_csv(PATHS["batter_home_final"])
        df_startingpitchers = pd.read_csv(PATHS["startingpitchers_final"])
        df_pitchers_normalized = pd.read_csv(PATHS["pitchers_normalized_cleaned"])
        df_weather_input = pd.read_csv(PATHS["weather_input"])
        df_weather_adjustments = pd.read_csv(PATHS["weather_adjustments"])
        df_stadium_metadata = pd.read_csv(PATHS["stadium_metadata"])

        # Create final_scores_projected if it doesn't exist or load it
        try:
            df_final_scores_projected = pd.read_csv(PATHS["final_scores_projected"])
        except FileNotFoundError:
            df_final_scores_projected = pd.DataFrame() # Create empty if not found

    except FileNotFoundError as e:
        print(f"Error: One of the input files was not found. Please check the path: {e}")
        return
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return

    print("Successfully loaded all input files.")

    # --- Task 1: Create columns and fill from bat_today_final ---
    print("Task 1: Creating and filling b_total_bases and b_rbi columns...")
    
    # Ensure player_id is present for merging
    if 'player_id' not in df_bat_today.columns:
        print("Warning: 'player_id' column not found in bat_today_final. Skipping Task 1.")
    else:
        for df in [df_batter_away, df_batter_home]:
            for col in ['b_total_bases', 'b_rbi']:
                if col not in df.columns:
                    df[col] = np.nan # Create column with NaNs if it doesn't exist
                
                # Merge to fill in values. Using a left merge to keep all rows from current df.
                # Only merge if the column exists in df_bat_today and player_id exists in both.
                if col in df_bat_today.columns:
                    df = df.merge(
                        df_bat_today[['player_id', col]],
                        on='player_id',
                        how='left',
                        suffixes=('', '_from_bat_today')
                    )
                    # Fill NaN values in the original column with values from the merged column
                    df[col] = df[col].fillna(df[f'{col}_from_bat_today'])
                    # Drop the temporary merged column
                    df.drop(columns=[f'{col}_from_bat_today'], inplace=True)
                else:
                    print(f"Warning: Column '{col}' not found in bat_today_final for filling.")

        # Reassign dataframes after merge as merge returns a new DataFrame
        df_batter_away = df_batter_away
        df_batter_home = df_batter_home
    
    print("Task 1 complete.")

    # --- Task 2: Delete columns from batter_home_final.csv ---
    print("Task 2: Deleting columns from batter_home_final.csv...")
    cols_to_delete_home = [
        'Park Factor_input', 'city_input', 'is_dome_input', 'state_input',
        'time_of_day_input', 'timezone_input', 'team', 'pitcher_home', 'pitcher_away'
    ]
    for col in cols_to_delete_home:
        if col in df_batter_home.columns:
            df_batter_home.drop(columns=[col], inplace=True)
            print(f"Deleted '{col}' from batter_home_final.csv")
    print("Task 2 complete.")

    # --- Task 3: Delete columns from batter_away_final.csv ---
    print("Task 3: Deleting columns from batter_away_final.csv...")
    cols_to_delete_away = ['pitcher_away', 'pitcher_home']
    for col in cols_to_delete_away:
        if col in df_batter_away.columns:
            df_batter_away.drop(columns=[col], inplace=True)
            print(f"Deleted '{col}' from batter_away_final.csv")
    print("Task 3 complete.")

    # --- Task 4: Delete columns from startingpitchers_final.csv ---
    print("Task 4: Deleting columns from startingpitchers_final.csv...")
    cols_to_delete_sp = ['last_name_first_name', 'year', 'team_xtra']
    for col in cols_to_delete_sp:
        if col in df_startingpitchers.columns:
            df_startingpitchers.drop(columns=[col], inplace=True)
            print(f"Deleted '{col}' from startingpitchers_final.csv")
    print("Task 4 complete.")

    # --- Task 5: Add columns to startingpitchers_final.csv if they don't exist ---
    print("Task 5: Adding columns to startingpitchers_final.csv...")
    cols_to_add_sp = ['home_run', 'park_factor', 'weather_factor', 'player_id']
    for col in cols_to_add_sp:
        if col not in df_startingpitchers.columns:
            df_startingpitchers[col] = np.nan
            print(f"Added '{col}' to startingpitchers_final.csv")
    print("Task 5 complete.")

    # --- Task 6: Inject Park Factor into startingpitchers_final ---
    print("Task 6: Injecting 'Park Factor' into startingpitchers_final.csv...")
    # Melt weather_input to easily match 'team' from startingpitchers to 'home_team' or 'away_team'
    df_weather_input_melted = pd.melt(df_weather_input, 
                                       id_vars=['Park Factor'], 
                                       value_vars=['home_team', 'away_team'],
                                       var_name='team_type', 
                                       value_name='team_match')
    df_weather_input_melted = df_weather_input_melted[['Park Factor', 'team_match']].drop_duplicates()

    df_startingpitchers = df_startingpitchers.merge(
        df_weather_input_melted,
        left_on='team',
        right_on='team_match',
        how='left'
    )
    df_startingpitchers['park_factor'] = df_startingpitchers['park_factor'].fillna(df_startingpitchers['Park Factor'])
    df_startingpitchers.drop(columns=['Park Factor', 'team_match'], inplace=True)
    print("Task 6 complete.")

    # --- Task 7: Inject Weather Factor into startingpitchers_final ---
    print("Task 7: Injecting 'weather_factor' into startingpitchers_final.csv...")
    # Melt weather_adjustments similar to weather_input
    df_weather_adjustments_melted = pd.melt(df_weather_adjustments,
                                            id_vars=['weather_factor'],
                                            value_vars=['home_team', 'away_team'],
                                            var_name='team_type',
                                            value_name='team_match')
    df_weather_adjustments_melted = df_weather_adjustments_melted[['weather_factor', 'team_match']].drop_duplicates()

    df_startingpitchers = df_startingpitchers.merge(
        df_weather_adjustments_melted,
        left_on='team',
        right_on='team_match',
        how='left'
    )
    df_startingpitchers['weather_factor'] = df_startingpitchers['weather_factor'].fillna(df_startingpitchers['weather_factor_y'])
    df_startingpitchers.drop(columns=['weather_factor_y', 'team_match'], inplace=True)
    print("Task 7 complete.")

    # --- Task 8: Inject player_id into startingpitchers_final ---
    # Note: There was a duplicate Task 8. Assuming the first one (player_id) is intended as 8,
    # and the second one (home_run) will be Task 9.
    print("Task 8: Injecting 'player_id' into startingpitchers_final.csv...")
    if 'last_name, first_name' in df_startingpitchers.columns and 'name' in df_pitchers_normalized.columns:
        df_startingpitchers = df_startingpitchers.merge(
            df_pitchers_normalized[['name', 'player_id']],
            left_on='last_name, first_name',
            right_on='name',
            how='left',
            suffixes=('', '_from_norm')
        )
        df_startingpitchers['player_id'] = df_startingpitchers['player_id'].fillna(df_startingpitchers['player_id_from_norm'])
        df_startingpitchers.drop(columns=['name', 'player_id_from_norm'], inplace=True)
    else:
        print("Warning: Skipping Task 8. 'last_name, first_name' not in startingpitchers_final or 'name' not in pitchers_normalized_cleaned.")
    print("Task 8 complete.")

    # --- Task 9 (Original Task 8 duplicate): Inject home_run into startingpitchers_final ---
    print("Task 9: Injecting 'home_run' into startingpitchers_final.csv...")
    if 'last_name, first_name' in df_startingpitchers.columns and 'name' in df_pitchers_normalized.columns:
        # Re-merge or use the existing merged df if Task 8 created it correctly,
        # ensuring 'home_run' from normalized pitchers is available.
        # For simplicity and clarity, we'll do a fresh merge for 'home_run'.
        df_startingpitchers = df_startingpitchers.merge(
            df_pitchers_normalized[['name', 'home_run']],
            left_on='last_name, first_name',
            right_on='name',
            how='left',
            suffixes=('', '_from_norm')
        )
        # Assuming 'home_run' in startingpitchers_final needs to be filled
        df_startingpitchers['home_run'] = df_startingpitchers['home_run'].fillna(df_startingpitchers['home_run_from_norm'])
        df_startingpitchers.drop(columns=['name_from_norm', 'home_run_from_norm'], errors='ignore', inplace=True)
    else:
        print("Warning: Skipping Task 9. 'last_name, first_name' not in startingpitchers_final or 'name' not in pitchers_normalized_cleaned.")
    print("Task 9 complete.")


    # --- Task 10: Copy home_team and away_team to final_scores_projected.csv ---
    print("Task 10: Copying home_team and away_team to final_scores_projected.csv...")
    
    # Ensure columns exist in stadium_metadata and final_scores_projected
    required_stadium_cols = ['home_team', 'away_team']
    if not all(col in df_stadium_metadata.columns for col in required_stadium_cols):
        print("Warning: 'home_team' or 'away_team' not found in stadium_metadata. Skipping Task 10.")
    else:
        # If final_scores_projected is empty, initialize it with these columns
        if df_final_scores_projected.empty:
            df_final_scores_projected = df_stadium_metadata[required_stadium_cols].copy()
        else:
            # Merge or update existing columns. Using a left merge to add if they don't exist
            # or update if they do, based on common index or a specific key if available.
            # For simplicity, if final_scores_projected is not empty, we assume it needs these
            # columns added/overwritten from stadium_metadata.
            # A more robust solution might require a specific join key if the rows don't align.
            
            # Simple approach: if they don't exist, add them directly.
            # If they exist, overwrite them assuming the intent is to refresh from stadium_metadata.
            for col in required_stadium_cols:
                if col in df_final_scores_projected.columns:
                    # Overwrite existing column
                    df_final_scores_projected[col] = df_stadium_metadata[col].copy()
                else:
                    # Add new column
                    df_final_scores_projected[col] = df_stadium_metadata[col].copy()
        
    print("Task 10 complete.")


    # --- Save Updated DataFrames ---
    print("Saving updated dataframes...")
    try:
        df_batter_away.to_csv(PATHS["batter_away_final"], index=False)
        df_batter_home.to_csv(PATHS["batter_home_final"], index=False)
        df_startingpitchers.to_csv(PATHS["startingpitchers_final"], index=False)
        df_final_scores_projected.to_csv(PATHS["final_scores_projected"], index=False)
        print("All updated files saved successfully.")
    except Exception as e:
        print(f"An error occurred while saving files: {e}")

if __name__ == "__main__":
    project_prep()
