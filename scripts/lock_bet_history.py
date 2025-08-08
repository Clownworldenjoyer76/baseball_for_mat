import pandas as pd
from datetime import datetime
import os

def lock_bet_history():
    """
    Creates a dated backup of the game and player props history CSV files.
    """
    
    # Get today's date in YYYY-MM-DD format
    todays_date = datetime.now().strftime('%Y-%m-%d')
    
    # Define input and output paths
    game_props_input = 'data/bets/game_props_history.csv'
    player_props_input = 'data/bets/player_props_history.csv'
    output_directory = 'data/bets/bet_history'
    
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    # Define the output filenames
    game_props_output = os.path.join(output_directory, f'{todays_date}_game_props.csv')
    player_props_output = os.path.join(output_directory, f'{todays_date}_player_props.csv')

    # Read the input files and save copies to the output directory
    try:
        # Process game props file
        game_props_df = pd.read_csv(game_props_input)
        game_props_df.to_csv(game_props_output, index=False)
        print(f"Successfully locked game props history to: {game_props_output}")

        # Process player props file
        player_props_df = pd.read_csv(player_props_input)
        player_props_df.to_csv(player_props_output, index=False)
        print(f"Successfully locked player props history to: {player_props_output}")

    except FileNotFoundError as e:
        print(f"Error: The file {e.filename} was not found. Please check your file paths.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    lock_bet_history()
