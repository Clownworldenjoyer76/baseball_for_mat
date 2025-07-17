import pandas as pd

# File paths
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
PARK_DAY_FILE = "data/Data/park_factors_day.csv"
PARK_NIGHT_FILE = "data/Data/park_factors_night.csv"

OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY_FILE = "data/adjusted/pitchers_away_park.csv"
LOG_HOME = "log_pitchers_home_park.txt"
LOG_AWAY = "log_pitchers_away_park.txt"

STATS_TO_ADJUST = [
    'home_run',
    'slg_percent',
    'xslg',
    'woba',
    'xwoba',
    'barrel_batted_rate',
    'hard_hit_percent'
]

def load_park_factors():
    park_day = pd.read_csv(PARK_DAY_FILE)
    park_night = pd.read_csv(PARK_NIGHT_FILE)
    park_day['home_team'] = park_day['home_team'].astype(str).str.strip().str.lower()
    park_night['home_team'] = park_night['home_team'].astype(str).str.strip().str.lower()
    return park_day, park_night

def get_park_factor(park_day, park_night, home_team, game_time):
    hour = int(str(game_time).split(":")[0])
    park_df = park_day if hour < 18 else park_night
    row = park_df[park_df['home_team'] == home_team.lower()]
    if not row.empty and not pd.isna(row['Park Factor'].values[0]):
        return float(row['Park Factor'].values[0])
    return None

def normalize_columns(df):
    df.columns = df.columns.str.strip()
    return df

def apply_adjustments(pitchers_df, games_df, side, park_day, park_night):
    adjusted = []
    log_entries = []

    team_key = 'home_team' if side == 'home' else 'away_team'
    pitchers_df = normalize_columns(pitchers_df)
    games_df = normalize_columns(games_df)

    pitchers_df[team_key] = pitchers_df[team_key].astype(str).str.strip().str.lower()

    for _, row in games_df.iterrows():
        try:
            home_team = str(row['home_team']).strip().lower()
            away_team = str(row['away_team']).strip().lower()
            game_time = str(row['game_time']).strip()

            if home_team in ["", "undecided", "nan"] or game_time in ["", "nan"]:
                log_entries.append(f"Skipping invalid game row — home_team: '{home_team}', game_time: '{game_time}'")
                continue

            team_name = home_team if side == 'home' else away_team
            park_factor = get_park_factor(park_day, park_night, home_team, game_time)

            if park_factor is None:
                log_entries.append(f"No park factor found for {home_team} at {game_time}")
                continue

            team_pitchers = pitchers_df[pitchers_df[team_key] == team_name].copy()
            if team_pitchers.empty:
                log_entries.append(f"No pitcher data for {team_name} ({side})")
                continue

            for stat in STATS_TO_ADJUST:
                if stat in team_pitchers.columns:
                    team_pitchers[stat] *= park_factor / 100
                else:
                    log_entries.append(f"Missing '{stat}' for {team_name}")

            if 'woba' in team_pitchers.columns:
                team_pitchers['adj_woba_park'] = team_pitchers['woba'] * park_factor / 100
            else:
                team_pitchers['adj_woba_park'] = None
                log_entries.append(f"Missing 'woba' — could not calculate adj_woba_park for {team_name}")

            adjusted.append(team_pitchers)
            log_entries.append(f"Adjusted {team_name} ({side}) with park factor {park_factor:.2f}")
        except Exception as e:
            log_entries.append(f"Error processing game row: {e}")
            continue

    if adjusted:
        result = pd.concat(adjusted, ignore_index=True)
        try:
            top5 = result[['name', team_key, 'adj_woba_park']]                 .sort_values(by='adj_woba_park', ascending=False).head(5)
            log_entries.append("\nTop 5 affected pitchers:")
            log_entries.append(top5.to_string(index=False))
        except Exception as e:
            log_entries.append(f"Could not compute Top 5: {e}")
    else:
        result = pd.DataFrame()
        log_entries.append("No pitchers adjusted.")

    return result, log_entries

def main():
    try:
        games_df = pd.read_csv(GAMES_FILE)
        pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)
        pitchers_away = pd.read_csv(PITCHERS_AWAY_FILE)
    except Exception as e:
        with open(LOG_HOME, "w") as f:
            f.write(f"❌ Error loading files: {e}\n")
        with open(LOG_AWAY, "w") as f:
            f.write(f"❌ Error loading files: {e}\n")
        return

    park_day, park_night = load_park_factors()

    adj_home, log_home = apply_adjustments(pitchers_home, games_df, "home", park_day, park_night)
    adj_away, log_away = apply_adjustments(pitchers_away, games_df, "away", park_day, park_night)

    adj_home.to_csv(OUTPUT_HOME_FILE, index=False)
    adj_away.to_csv(OUTPUT_AWAY_FILE, index=False)

    with open(LOG_HOME, "w") as f:
        f.writelines([line + "\n" for line in log_home])

    with open(LOG_AWAY, "w") as f:
        f.writelines([line + "\n" for line in log_away])

if __name__ == "__main__":
    main()
