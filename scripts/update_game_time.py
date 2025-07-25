import pandas as pd
import os

STADIUM_FILE = "data/Data/stadium_metadata.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PARK_DAY = "data/Data/park_factors_day.csv"
PARK_NIGHT = "data/Data/park_factors_night.csv"
LOG_FILE = "data/logs/game_time_update_log.csv"

def determine_time_of_day(game_time):
    try:
        hour = int(game_time.split(":")[0])
        if "PM" in game_time and hour >= 7:
            return "night"
        elif "AM" in game_time:
            return "day"
        elif "PM" in game_time:
            return "day"
        else:
            return "day"
    except:
        return "unknown"

def update_game_time_and_park_factors():
    stadium_df = pd.read_csv(STADIUM_FILE)
    games_df = pd.read_csv(GAMES_FILE)
    day_df = pd.read_csv(PARK_DAY)
    night_df = pd.read_csv(PARK_NIGHT)

    # Normalize team name formatting
    stadium_df['home_team'] = stadium_df['home_team'].str.strip()
    games_df['home_team'] = games_df['home_team'].str.strip()
    games_df['away_team'] = games_df['away_team'].str.strip()
    games_df['game_time'] = games_df['game_time'].astype(str).str.strip()

    if 'game_time' not in stadium_df.columns:
        stadium_df['game_time'] = ''
    if 'away_team' not in stadium_df.columns:
        stadium_df['away_team'] = ''
    if 'Park Factor' not in stadium_df.columns:
        stadium_df['Park Factor'] = ''
    if 'time_of_day' not in stadium_df.columns:
        stadium_df['time_of_day'] = ''

    updated_rows = []

    for idx, row in stadium_df.iterrows():
        team = row['home_team']
        match = games_df[games_df['home_team'] == team]

        if not match.empty:
            game_time = match.iloc[0]['game_time']
            away_team = match.iloc[0]['away_team']
            time_of_day = determine_time_of_day(game_time)

            # Select correct park factor
            if time_of_day == "day":
                pf_match = day_df[day_df['home_team'] == team]
            else:
                pf_match = night_df[night_df['home_team'] == team]

            park_factor = pf_match['Park Factor'].values[0] if not pf_match.empty else ''

            stadium_df.at[idx, 'game_time'] = game_time
            stadium_df.at[idx, 'away_team'] = away_team
            stadium_df.at[idx, 'Park Factor'] = park_factor
            stadium_df.at[idx, 'time_of_day'] = time_of_day
            updated_rows.append((team, away_team, game_time, park_factor, time_of_day))
        else:
            stadium_df.at[idx, 'game_time'] = ''
            stadium_df.at[idx, 'away_team'] = ''
            stadium_df.at[idx, 'Park Factor'] = ''
            stadium_df.at[idx, 'time_of_day'] = ''
            updated_rows.append((team, '', '', '', ''))

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    stadium_df.to_csv(STADIUM_FILE, index=False)
    pd.DataFrame(
        updated_rows, 
        columns=["home_team", "away_team", "game_time", "Park Factor", "time_of_day"]
    ).to_csv(LOG_FILE, index=False)

    print(f"✅ Game times and park factors updated for {len(updated_rows)} teams.")

if __name__ == "__main__":
    update_game_time_and_park_factors()
