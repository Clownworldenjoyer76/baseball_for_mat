import pandas as pd
import subprocess

# File paths
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
WEATHER_FILE = "data/weather_adjustments.csv"
OUTPUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_weather.csv"
LOG_HOME = "log_pitchers_home_weather.txt"
LOG_AWAY = "log_pitchers_away_weather.txt"

def apply_adjustment(df, team_col, stadium_map, weather_df):
    df = df.merge(stadium_map, on=team_col, how='left')
    df = df.merge(weather_df, on='venue', how='left')

    if 'woba' in df.columns and 'temperature' in df.columns:
        df['adj_woba_weather'] = df['woba'] * df['temperature'].apply(
            lambda x: 1.02 if x > 85 else 0.98 if x < 60 else 1.0
        )
    else:
        df['adj_woba_weather'] = None
    return df

def save_outputs(df, log_file, label):
    top5 = df.sort_values('adj_woba_weather', ascending=False).head(5)
    with open(log_file, "w") as f:
        f.write(f"Top 5 {label} Pitchers by adj_woba_weather:\n")
        cols = ["name", "team", "woba", "temperature", "adj_woba_weather"]
        for col in cols:
            if col not in df.columns:
                df[col] = None
        f.write(top5[cols].to_string(index=False))

def git_commit_and_push(files):
    try:
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", "Auto-update pitcher weather adjustment"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git push failed: {e}")

def main():
    pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)
    pitchers_away = pd.read_csv(PITCHERS_AWAY_FILE)
    stadiums = pd.read_csv(STADIUM_FILE)
    weather = pd.read_csv(WEATHER_FILE)

    home_map = stadiums[['home_team', 'venue']].rename(columns={'home_team': 'team'})
    away_map = stadiums[['away_team', 'venue']].rename(columns={'away_team': 'team'})

    adj_home = apply_adjustment(pitchers_home, 'team', home_map, weather)
    adj_away = apply_adjustment(pitchers_away, 'team', away_map, weather)

    adj_home.to_csv(OUTPUT_HOME, index=False)
    adj_away.to_csv(OUTPUT_AWAY, index=False)

    save_outputs(adj_home, LOG_HOME, "Home")
    save_outputs(adj_away, LOG_AWAY, "Away")

    git_commit_and_push([OUTPUT_HOME, OUTPUT_AWAY, LOG_HOME, LOG_AWAY])

if __name__ == "__main__":
    main()
