import pandas as pd
from pathlib import Path
import subprocess

def combine_adjustments(weather_path, park_path, output_path):
    weather_df = pd.read_csv(weather_path)
    park_df = pd.read_csv(park_path)
    merged = pd.merge(weather_df, park_df, on=["name", "home_team"], how="inner", suffixes=("_weather", "_park"))
    merged.to_csv(output_path, index=False)
    return len(merged)

def commit_file(path):
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", str(path)], check=True)
        subprocess.run(["git", "commit", "-m", f"Auto-commit: created {path.name}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"✅ Committed {path.name}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed for {path.name}: {e}")

def main():
    Path("data/adjusted").mkdir(parents=True, exist_ok=True)

    count_home = combine_adjustments(
        "data/adjusted/pitchers_home_weather.csv",
        "data/adjusted/pitchers_home_park.csv",
        "data/adjusted/pitchers_home_weather_park.csv"
    )
    commit_file(Path("data/adjusted/pitchers_home_weather_park.csv"))

    count_away = combine_adjustments(
        "data/adjusted/pitchers_away_weather.csv",
        "data/adjusted/pitchers_away_park.csv",
        "data/adjusted/pitchers_away_weather_park.csv"
    )
    commit_file(Path("data/adjusted/pitchers_away_weather_park.csv"))

    print(f"✅ Merged and saved: {count_home} home pitchers, {count_away} away pitchers")

if __name__ == "__main__":
    main()
