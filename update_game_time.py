
import pandas as pd

metadata_path = "data/Data/stadium_metadata.csv"
pitchers_path = "data/daily/todays_pitchers.csv"

stadium_df = pd.read_csv(metadata_path)
pitchers_df = pd.read_csv(pitchers_path)

pitchers_df.rename(columns=lambda x: x.strip().lower(), inplace=True)

# Clear all values in game_time column
if 'game_time' not in stadium_df.columns:
    stadium_df['game_time'] = ''
else:
    stadium_df['game_time'] = ''

# Map home_team to game_time from today's pitchers
if 'home_team' in pitchers_df.columns and 'game_time' in pitchers_df.columns:
    time_map = pitchers_df.set_index('home_team')['game_time'].to_dict()
    stadium_df['game_time'] = stadium_df['home_team'].map(time_map)

stadium_df.to_csv(metadata_path, index=False)

print("Updated stadium_metadata.csv:")
print(df[['team', 'game_time']])
