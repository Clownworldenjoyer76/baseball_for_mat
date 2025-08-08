# bet_tracker.py

import pandas as pd
import os

# Define file paths
# Input
BATTER_PROPS_FILE = 'data/_projections/batter_props_z_expanded.csv'
PITCHER_PROPS_FILE = 'data/_projections/pitcher_mega_z.csv'
FINAL_SCORES_FILE = 'data/_projections/final_scores_projected.csv'

# Output
GAME_PROPS_OUT = 'data/bets/game_props_history.csv'
PLAYER_PROPS_OUT = 'data/bets/player_props_history.csv'

def ensure_directory_exists(file_path):
    """Ensures the directory for a given file path exists."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def run_bet_tracker():
    """
    Processes daily projection data to generate game and player bet trackers.
    """
    
    # --- Part 1: Player Props ---
    
    # Read input data
    try:
        batter_df = pd.read_csv(BATTER_PROPS_FILE)
        pitcher_df = pd.read_csv(PITCHER_PROPS_FILE)
        games_df = pd.read_csv(FINAL_SCORES_FILE)
    except FileNotFoundError as e:
        print(f"Error: Required input file not found - {e}")
        return

    # Find the single date for today's data
    current_date = games_df['date'].iloc[0]

    # Find the overall best props
    # Top 2 batters with highest over_probability
    best_batters = batter_df.sort_values(by='over_probability', ascending=False).head(2).copy()
    best_batters['prop_type'] = 'Best Prop'
    
    # Top 1 pitcher with highest over_probability
    best_pitcher = pitcher_df.sort_values(by='over_probability', ascending=False).head(1).copy()
    best_pitcher['prop_type'] = 'Best Prop'

    # Combine best props and get their names to avoid duplicates
    best_props_df = pd.concat([best_batters, best_pitcher], ignore_index=True)
    best_prop_players = best_props_df['player_name'].unique()

    # Find individual game props for all other games
    individual_props_list = []
    
    # Get a list of all games from the final scores file
    games = games_df[['home_team', 'away_team']].drop_duplicates()

    for _, game in games.iterrows():
        home = game['home_team']
        away = game['away_team']

        # Filter batters for the specific game, excluding best prop players
        game_batters = batter_df[
            ((batter_df['team'] == home) | (batter_df['team'] == away)) &
            (~batter_df['player_name'].isin(best_prop_players))
        ].sort_values(by='over_probability', ascending=False).head(2)
        game_batters['prop_type'] = 'Individual Game'
        individual_props_list.append(game_batters)
        
        # Filter pitchers for the specific game, excluding best prop players
        game_pitchers = pitcher_df[
            ((pitcher_df['team'] == home) | (pitcher_df['team'] == away)) &
            (~pitcher_df['player_name'].isin(best_prop_players))
        ].sort_values(by='over_probability', ascending=False).head(1)
        game_pitchers['prop_type'] = 'Individual Game'
        individual_props_list.append(game_pitchers)
        
    # Combine all individual game props
    if individual_props_list:
        individual_props_df = pd.concat(individual_props_list, ignore_index=True)
        # Combine all props for today
        all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    else:
        all_props = best_props_df

    # Select and rename columns to match the output schema
    player_props_to_save = all_props[[
        'date', 'player_name', 'team', 'prop_line', 'prop_type'
    ]]
    # Add a column for prop_correct, which is not filled by this script
    player_props_to_save['prop_correct'] = ''

    # Append data to the player props history file
    ensure_directory_exists(PLAYER_PROPS_OUT)
    if not os.path.exists(PLAYER_PROPS_OUT):
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=True)
    else:
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=False, mode='a')

    # --- Part 2: Game Props ---

    # Create a new DataFrame for game props
    game_props_to_save = games_df[[
        'date', 'home_team', 'away_team'
    ]].copy()

    # Determine the favorite and projected total runs
    game_props_to_save['favorite'] = games_df.apply(
        lambda row: row['home_team'] if row['home_score'] > row['away_score'] else row['away_team'], axis=1
    )
    game_props_to_save['projected_real_run_total'] = games_df['home_score'] + games_df['away_score']
    
    # Add other columns from the output schema, leaving them empty
    game_props_to_save['favorite_correct'] = ''
    game_props_to_save['actual_real_run_total'] = ''
    game_props_to_save['run_total_diff'] = ''
    
    # Append data to the game props history file
    ensure_directory_exists(GAME_PROPS_OUT)
    if not os.path.exists(GAME_PROPS_OUT):
        game_props_to_save.to_csv(GAME_PROPS_OUT, index=False, header=True)
    else:
        game_props_to_save.to_csv(GAME_PROPS_OUT, index=False, header=False, mode='a')

    print(f"Bet tracker script finished successfully for date: {current_date}")

if __name__ == '__main__':
    run_bet_tracker()

