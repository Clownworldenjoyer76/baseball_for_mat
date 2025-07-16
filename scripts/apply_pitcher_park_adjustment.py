import pandas as pd

# File paths
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
PARK_DAY_FILE = "data/Data/park_factors_day.csv"
PARK_NIGHT_FILE = "data/Data/park_factors_night.csv"
TEAM_NAME_MAP_FILE = "data/Data/team_name_master.csv"

# Output files
OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY_FILE = "data/adjusted/pitchers_away_park.csv"
LOG_HOME = "log_pitchers_home_park.txt"
LOG_AWAY = "log_pitchers_away_park.txt"

STATS_TO_ADJUST = ['home_run', 'slug_percent', 'xslg', 'woba', 'xwoba', 'barrel_batted_rate', 'hard_hit_percent']

def load_park_factors(game_time):
    try:
        hour = int(str(game_time).split(':')[0])
        return pd.read_csv(PARK_DAY_FILE) if hour < 18 else pd.read_csv(PARK_NIGHT_FILE)
    except Exception as e:
        raise ValueError(f"Invalid game_time format: {game_time}") from e

def normalize_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def standardize_team_names(df, column, team_map):
    df = df.copy()
    df[column] = df[column].str.lower().str.strip()
    team_map = team_map.copy()
    team_map['team_code'] = team_map['team_code'].str.lower().str.strip()
    team_map['team_name'] = team_map['team_name'].str.strip()
    df = df.merge(team_map[['team_code', 'team_name']], how='left', left_on=column, right_on='team_code')
    df[column] = df['team_name']
    df.drop(columns=['team_code', 'team_name'], inplace=True)
    return df

def apply_adjustments(pitchers_df, games_df, team_name_map, side):
    adjusted = []
    log_entries = []

    pitchers_df = normalize_columns(pitchers_df)
    games_df = normalize_columns(games_df)
    team_name_map = normalize_columns(team_name_map)

    # Only standardize pitcher team names — game teams are already clean
    pitchers_df = standardize_team_names(pitchers_df, 'team', team_name_map)

    for _, row in games_df.iterrows():
        try:
            home_team = str(row['home_team']).strip()
            game_time = str(row['game_time']).strip()

            if home_team.lower() in ["", "undecided", "nan"] or game_time.lower() in ["", "nan"]:
                log_entries.append(
                    f"Skipping row due to invalid values — home_team: '{home_team}', game_time: '{game_time}'"
                )
                continue

            park_factors = load_park_factors(game_time)

            if 'home_team' not in park_factors.columns or 'Park Factor' not in park_factors.columns:
                log_entries.append("Park factors file is missing required columns.")
                continue

            park_factors['home_team'] = park_factors['home_team'].astype(str).str.lower().str.strip()
            park_row = park_factors[park_factors['home_team'] == home_team.lower()]

            if park_row.empty or pd.isna(park_row['Park Factor'].values[0]):
                log_entries.append(f"No park factor found for {home_team} at time {game_time}")
                continue

            park_factor = float(park_row['Park Factor'].values[0])

            team = row['home_team'] if side == 'home' else row.get('away_team', '')
            if pd.isna(team):
                log_entries.append(f"Missing {side} team name")
                continue

            team = str(team).strip()
            team_pitchers = pitchers_df[pitchers_df['team'].str.lower() == team.lower()].copy()

            if team_pitchers.empty:
                log_entries.append(f"No pitcher data found for team: {team}")
                continue

            for stat in STATS_TO_ADJUST:
                if stat in team_pitchers.columns:
                    team_pitchers[stat] = team_pitchers[stat] * (park_factor / 100)
                else:
                    log_entries.append(f"Stat '{stat}' not found in pitcher data for {team}")

            adjusted.append(team_pitchers)
            log_entries.append(f"Adjusted {team} pitchers using park factor {park_factor} at {home_team}")
        except Exception as e:
            log_entries.append(f"Error processing row: {e}")
            continue

    if adjusted:
        result = pd.concat(adjusted)
        try:
            top5 = result[['name', 'team', STATS_TO_ADJUST[0]]].sort_values(by=STATS_TO_ADJUST[0], ascending=False).head(5)
            log_entries.append('\nTop 5 affected pitchers:')
            log_entries.append(top5.to_string(index=False))
        except Exception as e:
            log_entries.append(f"Failed to log top 5 pitchers: {e}")
    else:
        result = pd.DataFrame()
        log_entries.append("No teams matched. No adjustments applied.")

    return result, log_entries

def main():
    games_df = pd.read_csv(GAMES_FILE)
    pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)
    pitchers_away = pd.read_csv(PITCHERS_AWAY_FILE)
    team_name_map = pd.read_csv(TEAM_NAME_MAP_FILE)

    adj_home, log_home = apply_adjustments(pitchers_home, games_df, team_name_map, side="home")
    adj_away, log_away = apply_adjustments(pitchers_away, games_df, team_name_map, side="away")

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
