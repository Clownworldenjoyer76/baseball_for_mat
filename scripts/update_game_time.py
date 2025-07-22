import pandas as pd
import os

STADIUM_FILE = "data/Data/stadium_metadata.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
LOG_FILE = "data/logs/game_time_update_log.csv"

def update_game_time():
    stadium_df = pd.read_csv(STADIUM_FILE)
    games_df = pd.read_csv(GAMES_FILE)

    # Normalize whitespace and case to Title Case
    stadium_df['home_team'] = stadium_df['home_team'].str.strip().str.title()
    games_df['home_team'] = games_df['home_team'].str.strip().str.title()
    games_df['away_team'] = games_df['away_team'].str.strip().str.title()
    games_df['game_time'] = games_df['game_time'].astype(str).str.strip()

    # Ensure columns exist
    if 'game_time' not in stadium_df.columns:
        stadium_df['game_time'] = ''
    if 'away_team' not in stadium_df.columns:
        stadium_df['away_team'] = ''

    updated_rows = []

    for idx, row in stadium_df.iterrows():
        team = row['home_team']
        match = games_df[games_df['home_team'] == team]

        if not match.empty:
            game_time = match.iloc[0]['game_time']
            away_team = match.iloc[0]['away_team']
            stadium_df.at[idx, 'game_time'] = game_time
            stadium_df.at[idx, 'away_team'] = away_team
            updated_rows.append((team, away_team, game_time))
        else:
            stadium_df.at[idx, 'game_time'] = ''
            stadium_df.at[idx, 'away_team'] = ''
            updated_rows.append((team, '', ''))

    # Ensure log directory exists
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    stadium_df.to_csv(STADIUM_FILE, index=False)
    pd.DataFrame(updated_rows, columns=["home_team", "away_team", "game_time"]).to_csv(LOG_FILE, index=False)

    print(f"✅ Game times updated for {len(updated_rows)} teams:")
    for team, opp, time in updated_rows:
        print(f" - {team} vs {opp}: {time if time else '[no game]'}")

if __name__ == "__main__":
    update_game_time()
