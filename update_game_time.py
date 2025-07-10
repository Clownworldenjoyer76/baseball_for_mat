import pandas as pd

# File paths
STADIUM_FILE = "data/Data/stadium_metadata.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"

def generate_game_time_updates():
    stadium_df = pd.read_csv(STADIUM_FILE)
    pitchers_df = pd.read_csv(PITCHERS_FILE)

    stadium_df['game_time'] = ''
    stadium_df['home_team'] = stadium_df['home_team'].str.strip()
    pitchers_df['home_team'] = pitchers_df['home_team'].str.strip()

    game_time_map = dict(zip(pitchers_df['home_team'], pitchers_df['game_time']))
    stadium_df['game_time'] = stadium_df['home_team'].map(game_time_map)

    stadium_df.to_csv(STADIUM_FILE, index=False)
    print("Updated stadium_metadata.csv")

if __name__ == "__main__":
    generate_game_time_updates()