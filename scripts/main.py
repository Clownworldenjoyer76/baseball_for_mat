# main.py
from fetch_data import fetch_player_props, fetch_game_props
from process_data import process_player_data, process_game_data
from update_history import update_player_history, update_game_history
from logging_utils import log

def run_bet_tracker():
    log("Fetching player props...")
    player_raw = fetch_player_props()
    log("Fetching game props...")
    game_raw = fetch_game_props()

    log("Processing player data...")
    player_df = process_player_data(player_raw)
    log("Processing game data...")
    game_df = process_game_data(game_raw)

    log("Updating player history...")
    update_player_history(player_df)
    log("Updating game history...")
    update_game_history(game_df)

if __name__ == "__main__":
    run_bet_tracker()
