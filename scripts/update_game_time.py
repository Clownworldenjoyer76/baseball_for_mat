import pandas as pd

STADIUM_FILE = "data/Data/stadium_metadata.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"

def update_game_time():
    stadium_df = pd.read_csv(STADIUM_FILE)
    games_df = pd.read_csv(GAMES_FILE)

    # Ensure 'game_time' column exists and is empty
    if 'game_time' not in stadium_df.columns:
        stadium_df['game_time'] = ''
    else:
        stadium_df['game_time'] = ''

    # Normalize whitespace
    stadium_df['home_team'] = stadium_df['home_team'].str.strip()
    games_df['home_team'] = games_df['home_team'].str.strip()

    updated_rows = []

    for idx, row in stadium_df.iterrows():
        home_team = row['home_team']
        match = games_df[games_df['home_team'] == home_team]

        if not match.empty:
            game_time = match.iloc[0]['game_time']
            stadium_df.at[idx, 'game_time'] = game_time
            updated_rows.append((home_team, game_time))
        else:
            print(f"⚠️ No match found for home_team: {home_team}")

    stadium_df.to_csv(STADIUM_FILE, index=False)
    pd.DataFrame(updated_rows, columns=["home_team", "game_time"]).to_csv("game_time_update_log.csv", index=False)

    print(f"✅ Updated game_time for {len(updated_rows)} teams:")
    for team, time in updated_rows:
        print(f" - {team}: {time}")

if __name__ == "__main__":
    update_game_time()
