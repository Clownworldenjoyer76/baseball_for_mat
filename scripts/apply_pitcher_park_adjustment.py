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

def load_park_factors(game_time):
    hour = int(str(game_time).split(":")[0])
    return pd.read_csv(PARK_DAY_FILE) if hour < 18 else pd.read_csv(PARK_NIGHT_FILE)

def normalize_columns(df):
    df.columns = df.columns.str.strip()
    return df

def apply_adjustments(pitchers_df, games_df, side):
    adjusted = []
    log_entries = []

    team_key = 'home_team' if side == 'home' else 'away_team'
    pitchers_df = normalize_columns(pitchers_df)
    games_df = normalize_columns(games_df)

    pitchers_df[team_key] = pitchers_df[team_key].astype(str).str.strip().str.lower()

    for _, row in games_df.iterrows():
        try:
            home_team = str(row['home_team']).strip()
            game_time = str(row['game_time']).strip()

            if not home_team or not game_time or home_team.lower() in ["undecided", "nan"] or game_time.lower() in ["nan"]:
                log_entries.append(f"Skipping row with invalid values — home_team: '{home_team}', game_time: '{game_time}'")
                continue

            park_factors = load_park_factors(game_time)
            park_factors['home_team'] = park_factors['home_team'].astype(str).str.strip().str.lower()

            if 'home_team' not in park_factors.columns or 'Park Factor' not in park_factors.columns:
                log_entries.append("Missing required columns in park factors file.")
                continue

            park_row = park_factors[park_factors['home_team'] == home_team.lower()]
            if park_row.empty or pd.isna(park_row['Park Factor'].values[0]):
                log_entries.append(f"No park factor found for {home_team} at {game_time}")
                continue

            park_factor = float(park_row['Park Factor'].values[0])
            team_name = home_team.lower() if side == "home" else str(row['away_team']).strip().lower()
            team_pitchers = pitchers_df[pitchers_df[team_key] == team_name].copy()

            if team_pitchers.empty:
                log_entries.append(f"No pitchers found for {team_name} ({side})")
                continue

            for stat in STATS_TO_ADJUST:
                if stat in team_pitchers.columns:
                    team_pitchers[stat] *= park_factor / 100
                else:
                    log_entries.append(f"Missing stat '{stat}' in pitcher data for {team_name}")

            adjusted.append(team_pitchers)
            log_entries.append(f"Adjusted {team_name} ({side}) using park factor {park_factor:.2f}")
        except Exception as e:
            log_entries.append(f"Error processing row: {e}")
            continue

    if adjusted:
        result = pd.concat(adjusted, ignore_index=True)
        try:
            top5 = result[['name', team_key, STATS_TO_ADJUST[0]]].sort_values(by=STATS_TO_ADJUST[0], ascending=False).head(5)
            log_entries.append("\nTop 5 affected pitchers:")
            log_entries.append(top5.to_string(index=False))
        except Exception as e:
            log_entries.append(f"Could not generate top 5 log: {e}")
    else:
        result = pd.DataFrame()
        log_entries.append("No pitchers adjusted.")

    return result, log_entries

def main():
    games_df = pd.read_csv(GAMES_FILE)
    pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)
    pitchers_away = pd.read_csv(PITCHERS_AWAY_FILE)

    adj_home, log_home = apply_adjustments(pitchers_home, games_df, side="home")
    adj_away, log_away = apply_adjustments(pitchers_away, games_df, side="away")

    adj_home.to_csv(OUTPUT_HOME_FILE, index=False)
    adj_away.to_csv(OUTPUT_AWAY_FILE, index=False)

    with open(LOG_HOME, "w") as f:
        f.writelines([line + "\n" for line in log_home])

    with open(LOG_AWAY, "w") as f:
        f.writelines([line + "\n" for line in log_away])

if __name__ == "__main__":
    main()
