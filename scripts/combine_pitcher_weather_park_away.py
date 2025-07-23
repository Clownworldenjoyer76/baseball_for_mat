import pandas as pd
from pathlib import Path
import subprocess

WEATHER_PATH = "data/adjusted/pitchers_away_weather.csv"
PARK_PATH = "data/adjusted/pitchers_away_park.csv"
OUTPUT_PATH = "data/adjusted/pitchers_away_weather_park.csv"
LOG_PATH = "summaries/combine_pitcher_weather_park_away.log"

def combine_adjustments():
    weather_df = pd.read_csv(WEATHER_PATH)
    park_df = pd.read_csv(PARK_PATH)

    merged = pd.merge(
        weather_df,
        park_df,
        on=["last_name, first_name", "home_team"],
        how="inner",
        suffixes=("_weather", "_park")
    )

    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)

    top5 = merged.sort_values("adj_woba_park", ascending=False).head(5)
    with open(LOG_PATH, "w") as f:
        for _, row in top5.iterrows():
            f.write(f"{row['last_name, first_name']} - {row['away_team']} - {row['adj_woba_park']:.3f}\n")

    return len(merged)

def commit_output():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", OUTPUT_PATH], check=True)
        subprocess.run(["git", "add", LOG_PATH], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: pitchers_away_weather_park outputs"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError:
        pass

def main():
    count = combine_adjustments()
    commit_output()
    print(f"✅ Combined {count} rows to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
