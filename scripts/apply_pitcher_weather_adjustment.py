import pandas as pd
from pathlib import Path
import subprocess

def apply_weather_adjustments_home(pitchers, weather):
    print("🔄 Applying weather adjustments to HOME pitchers...")
    pitchers['home_team'] = pitchers['team']
    weather = weather.drop_duplicates(subset='home_team')
    pitchers = pd.merge(pitchers, weather, on='home_team', how='left')

    if 'woba' not in pitchers.columns:
        print("⚠️ 'woba' column missing in pitchers. Defaulting to 0.320.")
        pitchers['woba'] = 0.320

    pitchers['adj_woba_weather'] = pitchers['woba'] + ((pitchers['temperature'] - 70) * 0.001)
    return pitchers.drop_duplicates()

def apply_weather_adjustments_away(pitchers, weather, todaysgames):
    print("🔄 Applying weather adjustments to AWAY pitchers...")
    away_team_to_home_team = todaysgames.set_index('away_team')['home_team'].to_dict()
    pitchers['home_team'] = pitchers['team'].map(away_team_to_home_team)
    weather = weather.drop_duplicates(subset='home_team')
    pitchers = pd.merge(pitchers, weather, on='home_team', how='left')

    if 'woba' not in pitchers.columns:
        print("⚠️ 'woba' column missing in pitchers. Defaulting to 0.320.")
        pitchers['woba'] = 0.320

    pitchers['adj_woba_weather'] = pitchers['woba'] + ((pitchers['temperature'] - 70) * 0.001)
    return pitchers.drop_duplicates()

def save_outputs(pitchers, label):
    print(f"💾 Saving adjusted pitchers: {label}")
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)

    outfile = out_path / f"pitchers_{label}_weather.csv"
    logfile = out_path / f"log_pitchers_weather_{label}.txt"

    pitchers.to_csv(outfile, index=False)

    try:
        top = pitchers[['last_name, first_name', 'team', 'adj_woba_weather']].head()
    except KeyError:
        print("❌ Column 'last_name, first_name' not found in pitcher DataFrame.")
        top = pitchers.head()

    with open(logfile, 'w') as f:
        f.write(str(top))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: adjusted pitchers weather + log"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    print("📂 Loading inputs...")
    weather = pd.read_csv("data/weather_adjustments.csv")
    todaysgames = pd.read_csv("data/raw/todaysgames_normalized.csv")

    pitchers_home = pd.read_csv("data/adjusted/pitchers_home.csv")
    adjusted_home = apply_weather_adjustments_home(pitchers_home, weather)
    save_outputs(adjusted_home, "home")

    pitchers_away = pd.read_csv("data/adjusted/pitchers_away.csv")
    adjusted_away = apply_weather_adjustments_away(pitchers_away, weather, todaysgames)
    save_outputs(adjusted_away, "away")

    commit_outputs()

if __name__ == "__main__":
    main()
