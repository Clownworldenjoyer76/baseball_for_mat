import pandas as pd
from pathlib import Path
from unidecode import unidecode
import os
import subprocess
import csv  # 🔧 Added to control quoting in CSV output

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

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    name = ','.join(part.strip() for part in name.split(','))
    return name.title()

def load_park_factors():
    try:
        park_day = pd.read_csv(PARK_DAY_FILE)
        park_night = pd.read_csv(PARK_NIGHT_FILE)
        park_day['home_team'] = park_day['home_team'].str.strip().str.lower()
        park_night['home_team'] = park_night['home_team'].str.strip().str.lower()
        return park_day, park_night
    except FileNotFoundError as e:
        print(f"❌ Error loading park factor files: {e}")
        return pd.DataFrame(columns=['home_team', 'Park Factor']), pd.DataFrame(columns=['home_team', 'Park Factor'])
    except Exception as e:
        print(f"❌ An unexpected error occurred loading park factor files: {e}")
        return pd.DataFrame(columns=['home_team', 'Park Factor']), pd.DataFrame(columns=['home_team', 'Park Factor'])

def get_park_factor(park_day, park_night, home_team, game_time):
    try:
        hour = int(str(game_time).split(":")[0])
    except (ValueError, TypeError):
        print(f"WARNING: Invalid game_time format for {home_team}: '{game_time}'")
        return None
    park_df = park_day if hour < 18 else park_night
    row = park_df[park_df['home_team'] == home_team.lower()] 
    if not row.empty and 'Park Factor' in row.columns and not pd.isna(row['Park Factor'].values[0]):
        return float(row['Park Factor'].values[0])
    return None

def normalize(df):
    if df is not None:
        df.columns = df.columns.str.strip()
    return df

def apply_adjustments(pitchers_df, games_df, side, park_day, park_night):
    adjusted = []
    logs = []

    required_games_cols = ['home_team', 'away_team', 'game_time']
    if not all(col in games_df.columns for col in required_games_cols):
        logs.append(f"❌ Missing required columns in games_df: {', '.join([col for col in required_games_cols if col not in games_df.columns])}")
        return pd.DataFrame(columns=list(pitchers_df.columns) + ['adj_woba_park']), logs

    team_key = "team"
    if team_key not in pitchers_df.columns:
        logs.append(f"❌ Missing required '{team_key}' column in pitchers_df for {side} side.")
        return pd.DataFrame(columns=list(pitchers_df.columns) + ['adj_woba_park']), logs

    pitchers_df[team_key] = pitchers_df[team_key].astype(str).str.strip().str.lower()

    for idx, row in games_df.iterrows():
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
                team_pitchers[stat] = pd.to_numeric(team_pitchers[stat], errors='coerce') 
                team_pitchers[stat] *= (park_factor / 100)
            else:
                logs.append(f"Missing '{stat}' in data for {team_name}")

        if 'woba' in team_pitchers.columns:
            team_pitchers['adj_woba_park'] = pd.to_numeric(team_pitchers['woba'], errors='coerce') * (park_factor / 100)
        else:
            team_pitchers['adj_woba_park'] = None
            logs.append(f"Missing 'woba' — could not compute adj_woba_park for {team_name}")

        adjusted.append(team_pitchers)
        logs.append(f"✅ Adjusted {team_name} ({side}) with park factor {park_factor:.2f}")

    if adjusted:
        df_result = pd.concat(adjusted, ignore_index=True)
        try:
            if 'adj_woba_park' in df_result.columns and not df_result['adj_woba_park'].isnull().all():
                top5 = df_result[['name', team_key, 'adj_woba_park']].sort_values(by='adj_woba_park', ascending=False).head(5)
                logs.append("\nTop 5 adjusted pitchers:")
                logs.append(top5.to_string(index=False))
            else:
                logs.append("⚠️ No valid 'adj_woba_park' values to generate top 5 log.")
        except Exception as e:
            logs.append(f"⚠️ Could not generate top 5 log: {e}")
    else:
        df_result = pd.DataFrame(columns=list(pitchers_df.columns) + ['adj_woba_park'])
        logs.append("⚠️ No pitchers adjusted.")

    return df_result, logs

def save_output(df, log, file_path, log_path):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False, quoting=csv.QUOTE_MINIMAL)  # 🔧 Fixed quoting
    with open(log_path, "w") as f:
        f.write("\n".join(log))
    print(f"✅ Wrote {file_path} with {len(df)} rows.")
    print(f"📝 Log written to {log_path}")

def git_commit_and_push():
    try:
        subprocess.run(["git", "add", "."], check=True, capture_output=True, text=True)
        status_output = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if not status_output.strip():
            print("✅ No changes to commit for pitcher park adjustments.")
        else:
            subprocess.run(["git", "commit", "-m", "📝 Apply pitcher park adjustment and update data files"], check=True, capture_output=True, text=True)
            subprocess.run(["git", "push"], check=True, capture_output=True, text=True)
            print("✅ Git commit and push complete for pitcher park adjustments.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed for pitcher park adjustments:")
        print(f"  Command: {e.cmd}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT: {e.stdout}")
        print(f"  STDERR: {e.stderr}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Git operations for pitcher park adjustments: {e}")

def main():
    try:
        games_df = normalize(pd.read_csv(GAMES_FILE))
        pitchers_home = normalize(pd.read_csv(PITCHERS_HOME_FILE))
        pitchers_away = normalize(pd.read_csv(PITCHERS_AWAY_FILE))
    except FileNotFoundError as e:
        print(f"❌ File not found during initial loading: {e}")
        return
    except Exception as e:
        print(f"❌ Error loading initial input files: {e}")
        return

    park_day, park_night = load_park_factors()

    if park_day.empty or park_night.empty:
        print("⚠️ Skipping park adjustments due to missing or empty park factor data.")
        save_output(pd.DataFrame(), ["No park adjustments due to missing park factor data."], OUTPUT_HOME_FILE, LOG_HOME)
        save_output(pd.DataFrame(), ["No park adjustments due to missing park factor data."], OUTPUT_AWAY_FILE, LOG_AWAY)
        git_commit_and_push()
        return

    adj_home, log_home = apply_adjustments(pitchers_home, games_df, "home", park_day, park_night)
    adj_away, log_away = apply_adjustments(pitchers_away, games_df, "away", park_day, park_night)

    save_output(adj_home, log_home, OUTPUT_HOME_FILE, LOG_HOME)
    save_output(adj_away, log_away, OUTPUT_AWAY_FILE, LOG_AWAY)

    git_commit_and_push()

if __name__ == "__main__":
    main()
