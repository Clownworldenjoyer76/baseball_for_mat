import pandas as pd
from pathlib import Path
import subprocess

def apply_weather_adjustments_home(batters, weather):
    batters['home_team'] = batters['team']
    weather = weather.drop_duplicates(subset='home_team')
    batters = pd.merge(batters, weather, on='home_team', how='left')

    # Assign real woba source if available
    if 'woba_weather' in batters.columns:
        batters['woba'] = batters['woba_weather']
    else:
        batters['woba'] = 0.320

    batters['adj_woba_weather'] = batters['woba'] + ((batters['temperature'] - 70) * 0.001)
    return batters

def apply_weather_adjustments_away(batters, weather, todaysgames):
    away_team_to_home_team = todaysgames.set_index('away_team')['home_team'].to_dict()
    batters['home_team'] = batters['team'].map(away_team_to_home_team)
    weather = weather.drop_duplicates(subset='home_team')
    batters = pd.merge(batters, weather, on='home_team', how='left')

    if 'woba_weather' in batters.columns:
        batters['woba'] = batters['woba_weather']
    else:
        batters['woba'] = 0.320

    batters['adj_woba_weather'] = batters['woba'] + ((batters['temperature'] - 70) * 0.001)
    return batters

def save_outputs(batters, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"batters_{label}_weather.csv"
    logfile = out_path / f"log_weather_{label}.txt"
    batters.to_csv(outfile, index=False)
    with open(logfile, 'w') as f:
        f.write(str(batters[['last_name, first_name', 'team', 'adj_woba_weather']].head()))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: adjusted batters + log"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    weather = pd.read_csv("data/weather_adjustments.csv")
    todaysgames = pd.read_csv("data/raw/todaysgames_normalized.csv")

    batters_home = pd.read_csv("data/adjusted/batters_home.csv")
    adjusted_home = apply_weather_adjustments_home(batters_home, weather)
    save_outputs(adjusted_home, "home")

    batters_away = pd.read_csv("data/adjusted/batters_away.csv")
    adjusted_away = apply_weather_adjustments_away(batters_away, weather, todaysgames)
    save_outputs(adjusted_away, "away")

    commit_outputs()

if __name__ == "__main__":
    main()
