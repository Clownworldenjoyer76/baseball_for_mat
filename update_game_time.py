
import pandas as pd

STADIUM_FILE = "data/Data/stadium_metadata.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"

def generate_game_time_updates():
    stadium_df = pd.read_csv(STADIUM_FILE)
    pitchers_df = pd.read_csv(PITCHERS_FILE)

    # Ensure 'game_time' column exists
    if 'game_time' not in stadium_df.columns:
        stadium_df['game_time'] = ''
    else:
        stadium_df['game_time'] = ''  # Clear all values

    # Normalize team names
    stadium_df['home_team'] = stadium_df['home_team'].str.strip()
    pitchers_df['home_team'] = pitchers_df['home_team'].str.strip()

    # Track update log
    updated_rows = []

    for idx, row in stadium_df.iterrows():
        home_team = row['home_team']
        match = pitchers_df[pitchers_df['home_team'] == home_team]

        if not match.empty:
            game_time = match.iloc[0]['game_time']
            stadium_df.at[idx, 'game_time'] = game_time
            updated_rows.append((home_team, game_time))
        else:
            print(f"⚠️ No match found for home_team: {home_team}")

    stadium_df.to_csv(STADIUM_FILE, index=False)

    # Save update log
    pd.DataFrame(updated_rows, columns=["home_team", "game_time"]).to_csv("game_time_update_log.csv", index=False)

    print(f"\n✅ Updated game_time for {len(updated_rows)} teams:")
    for team, time in updated_rows:
        print(f" - {team}: {time}")

if __name__ == "__main__":
    generate_game_time_updates()
