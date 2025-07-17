import pandas as pd

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
PARK_DAY_FILE = "data/Data/park_factors_day.csv"
PARK_NIGHT_FILE = "data/Data/park_factors_night.csv"

OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY_FILE = "data/adjusted/pitchers_away_park.csv"

def load_park_factors():
    park_day = pd.read_csv(PARK_DAY_FILE)
    park_night = pd.read_csv(PARK_NIGHT_FILE)
    park_day['home_team'] = park_day['home_team'].str.lower().str.strip()
    park_night['home_team'] = park_night['home_team'].str.lower().str.strip()
    return park_day, park_night

def get_park_factor(day, night, home_team, game_time):
    try:
        hour = pd.to_datetime(game_time).hour
    except:
        return None
    df = day if hour < 18 else night
    row = df[df['home_team'] == home_team.lower()]
    if row.empty: return None
    return float(row['Park Factor'].values[0])

def apply(pitchers, games, team_key, day, night):
    pitchers[team_key] = pitchers[team_key].str.lower().str.strip()
    adjusted = []
    for _, row in games.iterrows():
        game_time = str(row['game_time'])
        home_team = str(row['home_team']).strip().lower()
        away_team = str(row['away_team']).strip().lower()
        park = get_park_factor(day, night, home_team, game_time)
        if park is None: continue
        team = home_team if team_key == 'home_team' else away_team
        chunk = pitchers[pitchers[team_key] == team].copy()
        if chunk.empty: continue
        if 'woba' in chunk.columns:
            chunk['adj_woba_park'] = chunk['woba'] * park / 100
        else:
            chunk['adj_woba_park'] = None
        adjusted.append(chunk)
    return pd.concat(adjusted) if adjusted else pd.DataFrame()

def main():
    games = pd.read_csv(GAMES_FILE)
    home = pd.read_csv(PITCHERS_HOME_FILE)
    away = pd.read_csv(PITCHERS_AWAY_FILE)
    park_day, park_night = load_park_factors()
    adj_home = apply(home, games, 'home_team', park_day, park_night)
    adj_away = apply(away, games, 'away_team', park_day, park_night)
    adj_home.to_csv(OUTPUT_HOME_FILE, index=False)
    adj_away.to_csv(OUTPUT_AWAY_FILE, index=False)

if __name__ == "__main__":
    main()
