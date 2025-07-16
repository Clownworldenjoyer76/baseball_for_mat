import pandas as pd

# File paths
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PARK_DAY_FILE = "data/Data/park_factors_day.csv"
PARK_NIGHT_FILE = "data/Data/park_factors_night.csv"
OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_park.csv"
LOG_HOME = "log_pitchers_home_park.txt"

STATS_TO_ADJUST = [
    'home_run', 'slug_percent', 'xslg', 'woba',
    'xwoba', 'barrel_batted_rate', 'hard_hit_percent'
]

def load_park_factors(game_time):
    hour = int(str(game_time).split(':')[0])
    return pd.read_csv(PARK_DAY_FILE) if hour < 18 else pd.read_csv(PARK_NIGHT_FILE)

def normalize_columns(df):
    df.columns = df.columns.str.strip()
    return df

def apply_adjustments(pitchers_df, games_df):
    adjusted = []
    log_entries = []

    pitchers_df = normalize_columns(pitchers_df)
    games_df = normalize_columns(games_df)

    # Ensure lowercase for matching
    pitchers_df['home_team'] = pitchers_df['home_team'].astype(str).str.strip().str.lower()
    games_df['home_team'] = games_df['home_team'].astype(str).str.strip().str.lower()
    games_df['game_time'] = games_df['game_time'].astype(str).str.strip()

    for _, row in games_df.iterrows():
        home_team = row['home_team']
        game_time = row['game_time']

        if not home_team or not game_time:
            log_entries.append(f"Skipping row due to invalid values — home_team: '{home_team}', game_time: '{game_time}'")
            continue

        try:
            park_factors = load_park_factors(game_time)
            park_factors['home_team'] = park_factors['home_team'].astype(str).str.strip().str.lower()

            if 'home_team' not in park_factors.columns or 'Park Factor' not in park_factors.columns:
                log_entries.append("Park factors file is missing required columns.")
                continue

            park_row = park_factors[park_factors['home_team'] == home_team]

            if park_row.empty or pd.isna(park_row['Park Factor'].values[0]):
                log_entries.append(f"No park factor found for {home_team} at time {game_time}")
                continue

            park_factor = float(park_row['Park Factor'].values[0])
            team_pitchers = pitchers_df[pitchers_df['home_team'] == home_team].copy()

            if team_pitchers.empty:
                log_entries.append(f"No pitcher data found for team: {home_team}")
                continue

            for stat in STATS_TO_ADJUST:
                if stat in team_pitchers.columns:
                    team_pitchers[stat] *= park_factor / 100
                else:
                    log_entries.append(f"Stat '{stat}' not found for {home_team}")

            adjusted.append(team_pitchers)
            log_entries.append(f"Adjusted {home_team} pitchers using park factor {park_factor}")
        except Exception as e:
            log_entries.append(f"Error processing team {home_team}: {e}")
            continue

    if adjusted:
        result = pd.concat(adjusted)
        try:
            top5 = result[['name', 'home_team', STATS_TO_ADJUST[0]]].sort_values(by=STATS_TO_ADJUST[0], ascending=False).head(5)
            log_entries.append('\nTop 5 affected pitchers:')
            log_entries.append(top5.to_string(index=False))
        except Exception as e:
            log_entries.append(f"Failed to log top 5 pitchers: {e}")
    else:
        result = pd.DataFrame()
        log_entries.append("No pitchers adjusted. Output is empty.")

    return result, log_entries

def main():
    games_df = pd.read_csv(GAMES_FILE)
    pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)

    adj_home, log_home = apply_adjustments(pitchers_home, games_df)
    adj_home.to_csv(OUTPUT_HOME_FILE, index=False)

    with open(LOG_HOME, 'w') as f:
        for line in log_home:
            f.write(line + '\n')

if __name__ == "__main__":
    main()
