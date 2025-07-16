
import pandas as pd

# File paths
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
PARK_DAY_FILE = "data/Data/park_factors_day.csv"
PARK_NIGHT_FILE = "data/Data/park_factors_night.csv"

# Output files
OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY_FILE = "data/adjusted/pitchers_away_park.csv"
LOG_HOME = "log_pitchers_home_park.txt"
LOG_AWAY = "log_pitchers_away_park.txt"

STATS_TO_ADJUST = ['home_run', 'slug_percent', 'xslg', 'woba', 'xwoba', 'barrel_batted_rate', 'hard_hit_percent']

def load_park_factors(game_time):
    hour = int(game_time.split(':')[0])
    return pd.read_csv(PARK_DAY_FILE) if hour < 18 else pd.read_csv(PARK_NIGHT_FILE)

def apply_adjustments(pitchers_df, games_df, side):
    adjusted = []
    log_entries = []

    for _, row in games_df.iterrows():
        home_team = row['home_team']
        game_time = row['game_time']
        park_factors = load_park_factors(game_time)

        park_row = park_factors[park_factors['home_team'] == home_team]
        if park_row.empty:
            continue

        park_factor = float(park_row['Park Factor'].values[0])
        team = home_team if side == "home" else row['away_team']
        team_pitchers = pitchers_df[pitchers_df['team'] == team].copy()
        if team_pitchers.empty:
            continue

        for stat in STATS_TO_ADJUST:
            if stat in team_pitchers.columns:
                team_pitchers[stat] = team_pitchers[stat] * (park_factor / 100)

        adjusted.append(team_pitchers)
        log_entries.append(f"Adjusted {team} pitchers using park factor {park_factor} at {home_team}")

    if adjusted:
        result = pd.concat(adjusted)
        stat_to_log = STATS_TO_ADJUST[0] if STATS_TO_ADJUST else 'woba'
        try:
            top5 = result[['name', 'team', stat_to_log]].sort_values(by=stat_to_log, ascending=False).head(5)
            log_entries.append('\nTop 5 affected pitchers:')
            log_entries.append(top5.to_string(index=False))
        except Exception as e:
            log_entries.append(f"Failed to log top 5 pitchers: {e}")
    else:
        result = pd.DataFrame()

    return result, log_entries

def main():
    games_df = pd.read_csv(GAMES_FILE)
    pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)
    pitchers_away = pd.read_csv(PITCHERS_AWAY_FILE)

    adj_home, log_home = apply_adjustments(pitchers_home, games_df, side="home")
    adj_away, log_away = apply_adjustments(pitchers_away, games_df, side="away")

    adj_home.to_csv(OUTPUT_HOME_FILE, index=False)
    adj_away.to_csv(OUTPUT_AWAY_FILE, index=False)

    with open(LOG_HOME, 'w') as f:
        for line in log_home:
            f.write(line + '\n')

    with open(LOG_AWAY, 'w') as f:
        for line in log_away:
            f.write(line + '\n')

if __name__ == "__main__":
    main()
