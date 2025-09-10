import pandas as pd
import os

def create_column_if_not_exists(df, column_name):
    """
    Checks if a column exists in a DataFrame and creates it if not.
    """
    if column_name not in df.columns:
        print(f"Creating '{column_name}' column...")
        df[column_name] = None
    else:
        print(f"'{column_name}' column already exists. Skipping creation.")
    return df

def process_file(filepath, column_name):
    """
    Helper function to load a CSV, ensure a column exists, and save it.
    """
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}. Skipping.")
        return None
    
    df = pd.read_csv(filepath)
    df = create_column_if_not_exists(df, column_name)
    df.to_csv(filepath, index=False)
    print(f"Updated '{filepath}' with '{column_name}' column check.")
    return df

def inject_data_to_csv(target_df, source_df, on_col, inject_col, target_filepath):
    """
    Merges data from a source DataFrame into a target DataFrame and saves the result.
    """
    print(f"Injecting '{inject_col}' into '{target_filepath}'...")
    
    # Perform a left merge to add the new column
    merged_df = pd.merge(target_df, source_df[[on_col, inject_col]].drop_duplicates(), on=on_col, how='left')
    
    # If the column to be injected already existed from a previous step, drop the old one
    if f'{inject_col}_x' in merged_df.columns and f'{inject_col}_y' in merged_df.columns:
        merged_df[inject_col] = merged_df[f'{inject_col}_y']
        merged_df = merged_df.drop(columns=[f'{inject_col}_x', f'{inject_col}_y'])
    
    merged_df.to_csv(target_filepath, index=False)
    print(f"Successfully injected and saved to '{target_filepath}'.")
    return merged_df

def main():
    """
    Main function to run the data preparation workflow.
    """
    # Define file paths
    batter_props_projected_path = 'data/_projections/batter_props_projected_final.csv'
    batter_props_expanded_path = 'data/_projections/batter_props_expanded_final.csv'
    pitcher_props_projected_path = 'data/_projections/pitcher_props_projected_final.csv'
    batters_today_path = 'data/cleaned/batters_today.csv'
    mlb_sched_path = 'data/bets/mlb_sched.csv'
    
    print("--- Starting final_csvs_prep.py ---")

    # 1. & 2. Create 'team_id' and 'game_id' columns if they don't exist
    batter_props_projected_df = process_file(batter_props_projected_path, 'team_id')
    if batter_props_projected_df is not None:
        batter_props_projected_df = create_column_if_not_exists(batter_props_projected_df, 'game_id')
        batter_props_projected_df.to_csv(batter_props_projected_path, index=False)

    process_file(batter_props_expanded_path, 'game_id')
    process_file(pitcher_props_projected_path, 'game_id')
    
    # Load required data for injection
    if os.path.exists(batters_today_path) and batter_props_projected_df is not None:
        batters_today_df = pd.read_csv(batters_today_path)
        
        # 3. Inject team_id into batter_props_projected_final.csv
        inject_data_to_csv(batter_props_projected_df, batters_today_df, 'player_id', 'team_id', batter_props_projected_path)
    else:
        print(f"Skipping team_id injection. File not found: {batters_today_path} or {batter_props_projected_path}")

    # Load schedule for game_id injection
    if os.path.exists(mlb_sched_path):
        mlb_sched_df = pd.read_csv(mlb_sched_path)
        
        # Create a simplified schedule with a single team_id column
        home_games = mlb_sched_df[['game_id', 'home_team_id']].rename(columns={'home_team_id': 'team_id'})
        away_games = mlb_sched_df[['game_id', 'away_team_id']].rename(columns={'away_team_id': 'team_id'})
        simplified_sched_df = pd.concat([home_games, away_games]).drop_duplicates().reset_index(drop=True)
        
        # 4. Inject game_id into the specified CSVs
        if os.path.exists(batter_props_projected_path):
            batter_props_projected_df = pd.read_csv(batter_props_projected_path)
            inject_data_to_csv(batter_props_projected_df, simplified_sched_df, 'team_id', 'game_id', batter_props_projected_path)
        else:
            print(f"Skipping game_id injection for {batter_props_projected_path}. File not found.")

        if os.path.exists(batter_props_expanded_path):
            batter_props_expanded_df = pd.read_csv(batter_props_expanded_path)
            # Assuming 'batter_props_expanded_final.csv' has a 'team_id' column
            inject_data_to_csv(batter_props_expanded_df, simplified_sched_df, 'team_id', 'game_id', batter_props_expanded_path)
        else:
            print(f"Skipping game_id injection for {batter_props_expanded_path}. File not found.")
            
        if os.path.exists(pitcher_props_projected_path):
            pitcher_props_projected_df = pd.read_csv(pitcher_props_projected_path)
            # Assuming 'pitcher_props_projected_final.csv' has a 'team_id' column
            inject_data_to_csv(pitcher_props_projected_df, simplified_sched_df, 'team_id', 'game_id', pitcher_props_projected_path)
        else:
            print(f"Skipping game_id injection for {pitcher_props_projected_path}. File not found.")
    else:
        print(f"Skipping game_id injection. File not found: {mlb_sched_path}")

    print("--- Script finished. ---")

if __name__ == "__main__":
    main()

