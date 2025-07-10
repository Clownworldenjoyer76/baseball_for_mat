
import pandas as pd

# File paths
STADIUM_FILE = "data/Data/stadium_metadata.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"

def generate_game_time_updates():
    # Load CSVs
    stadium_df = pd.read_csv(STADIUM_FILE)
    pitchers_df = pd.read_csv(PITCHERS_FILE)

    # Clear existing game_time column (if it exists)
    if 'game_time' not in stadium_df.columns:
        stadium_df['game_time'] = ''
    else:
        stadium_df['game_time'] = ''

    # Strip whitespace from home_team names just in case
    stadium_df['home_team'] = stadium_df['home_team'].str.strip()
    pitchers_df['home_team'] = pitchers_df['home_team'].str.strip()

    # Build mapping from todays_pitchers.csv
    game_time_map = dict(zip(pitchers_df['home_team'], pitchers_df['game_time']))

    # Update game_time based on matching home_team
    stadium_df['game_time'] = stadium_df['home_team'].map(game_time_map)

    # Save the result
    stadium_df.to_csv(STADIUM_FILE, index=False)
    print("Updated stadium_metadata.csv")

if __name__ == "__main__":
    generate_game_time_updates()
