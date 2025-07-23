import pandas as pd
import subprocess

# File paths
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv"
OUTPUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_weather.csv"
LOG_HOME = "log_pitchers_home_weather.txt"
LOG_AWAY = "log_pitchers_away_weather.txt"

def adjust_temperature(temp):
    if pd.isna(temp):
        return 1.0
    if temp > 85:
        return 1.02
    elif temp < 60:
        return 0.98
    return 1.0

def apply_adjustment(df, team_col, weather_df, side):
    merged = df.merge(weather_df, left_on=team_col, right_on=team_col, how='left')
    if 'temperature' not in merged.columns:
        merged['adj_woba_weather'] = None
        merged['temperature'] = None
    else:
        merged['adj_woba_weather'] = merged['woba'] * merged['temperature'].apply(adjust_temperature)
    return merged

def log_top5(df, log_path, side):
    with open(log_path, "w") as f:
        f.write(f"Top 5 {side.capitalize()} Pitchers by adj_woba_weather:\n")
        if "adj_woba_weather" in df.columns:
            top5 = df.sort_values('adj_woba_weather', ascending=False).head(5)
            f.write(top5[["name", "team", "woba", "temperature", "adj_woba_weather"]].to_string(index=False))
        else:
            f.write("No adjusted wOBA data available.")

def git_commit_and_push(files):
    try:
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", "Auto-update pitcher weather adjustment"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git push failed: {e}")

def main():
    home_df = pd.read_csv(PITCHERS_HOME_FILE)
    away_df = pd.read_csv(PITCHERS_AWAY_FILE)
    weather_df = pd.read_csv(WEATHER_FILE)

    adjusted_home = apply_adjustment(home_df, "home_team", weather_df, "home")
    adjusted_away = apply_adjustment(away_df, "away_team", weather_df, "away")

    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    log_top5(adjusted_home, LOG_HOME, "home")
    log_top5(adjusted_away, LOG_AWAY, "away")

    git_commit_and_push([OUTPUT_HOME, OUTPUT_AWAY, LOG_HOME, LOG_AWAY])

if __name__ == "__main__":
    main()
