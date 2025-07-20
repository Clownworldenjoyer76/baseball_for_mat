import pandas as pd
from pathlib import Path
from unidecode import unidecode

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    name = ','.join(part.strip() for part in name.split(','))
    return name.title()

import Path
import os
import subprocess

# Change working directory to repo root
os.chdir(Path(__file__).resolve().parents[1])

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
    'home_run', 'slg_percent', 'xslg', 'woba', 'xwoba', 'barrel_batted_rate', 'hard_hit_percent'
]

def load_park_factors():
    park_day = pd.read_csv(PARK_DAY_FILE)
    park_night = pd.read_csv(PARK_NIGHT_FILE)
    park_day['home_team'] = park_day['home_team'].str.strip().str.lower()
    park_night['home_team'] = park_night['home_team'].str.strip().str.lower()
    return park_day, park_night

def get_park_factor(park_day, park_night, home_team, game_time):
    try:
        hour = int(str(game_time).split(":")[0])
    except:
        return None
    park_df = park_day if hour < 18 else park_night
    row = park_df[park_df['home_team'] == home_team.lower()]
    if not row.empty and not pd.isna(row['Park Factor'].values[0]):
        return float(row['Park Factor'].values[0])
    return None

def normalize(df):
    df.columns = df.columns.str.strip()
    return df

def apply_adjustments(pitchers_df, games_df, side, park_day, park_night):
    adjusted = []
    logs = []

    team_key = 'home_team' if side == 'home' else 'away_team'
    pitchers_df[team_key] = pitchers_df[team_key].astype(str).str.strip().str.lower()

    for _, row in games_df.iterrows():
        home_team = str(row['home_team']).strip().lower()
        away_team = str(row['away_team']).strip().lower()
        game_time = str(row['game_time']).strip()

        if home_team in ["", "undecided", "nan"] or game_time in ["", "nan"]:
            logs.append(f"Skipping invalid game — home_team: '{home_team}', game_time: '{game_time}'")
            continue

        team_name = home_team if side == 'home' else away_team
        park_factor = get_park_factor(park_day, park_night, home_team, game_time)

        if park_factor is None:
            logs.append(f"❌ No park factor found for {home_team} at {game_time}")
            continue

        team_pitchers = pitchers_df[pitchers_df[team_key] == team_name].copy()
        if team_pitchers.empty:
            logs.append(f"⚠️ No pitcher data for {team_name} ({side}) — key: {team_key}")
            continue

        for stat in STATS_TO_ADJUST:
            if stat in team_pitchers.columns:
                team_pitchers[stat] *= park_factor / 100
            else:
                logs.append(f"Missing '{stat}' in data for {team_name}")

        if 'woba' in team_pitchers.columns:
            team_pitchers['adj_woba_park'] = team_pitchers['woba'] * park_factor / 100
        else:
            team_pitchers['adj_woba_park'] = None
            logs.append(f"Missing 'woba' — could not compute adj_woba_park for {team_name}")

        adjusted.append(team_pitchers)
        logs.append(f"✅ Adjusted {team_name} ({side}) with park factor {park_factor:.2f}")

    if adjusted:
        df_result = pd.concat(adjusted, ignore_index=True)
        try:
            top5 = df_result[['name', team_key, 'adj_woba_park']].sort_values(by='adj_woba_park', ascending=False).head(5)
            logs.append("\nTop 5 adjusted pitchers:")
            logs.append(top5.to_string(index=False))
        except Exception as e:
            logs.append(f"⚠️ Could not generate top 5 log: {e}")
    else:
        df_result = pd.DataFrame(columns=list(pitchers_df.columns) + ['adj_woba_park'])
        logs.append("⚠️ No pitchers adjusted.")

    return df_result, logs

def save_output(df, log, file_path, log_path):
    df.to_csv(file_path, index=False)
    with open(log_path, "w") as f:
        f.write("\n".join(log))
    print(f"✅ Wrote {file_path} with {len(df)} rows.")
    print(f"📝 Log written to {log_path}")

def git_commit_and_push(files):
    try:
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", "Auto-update pitcher park adjustment"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git push failed: {e}")

def main():
    games_df = normalize(pd.read_csv(GAMES_FILE))
    pitchers_home = normalize(pd.read_csv(PITCHERS_HOME_FILE))
    pitchers_away = normalize(pd.read_csv(PITCHERS_AWAY_FILE))
    park_day, park_night = load_park_factors()

    adj_home, log_home = apply_adjustments(pitchers_home, games_df, "home", park_day, park_night)
    adj_away, log_away = apply_adjustments(pitchers_away, games_df, "away", park_day, park_night)

    save_output(adj_home, log_home, OUTPUT_HOME_FILE, LOG_HOME)
    save_output(adj_away, log_away, OUTPUT_AWAY_FILE, LOG_AWAY)

    git_commit_and_push([OUTPUT_HOME_FILE, OUTPUT_AWAY_FILE, LOG_HOME, LOG_AWAY])

if __name__ == "__main__":
    main()
