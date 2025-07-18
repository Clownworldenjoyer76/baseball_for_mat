import pandas as pd
from pathlib import Path
import subprocess

def combine_adjustments(label):
    weather_file = f"data/adjusted/batters_{label}_weather.csv"
    park_file = f"data/adjusted/batters_{label}_park.csv"
    output_file = f"data/adjusted/batters_{label}_weather_park.csv"

    df_weather = pd.read_csv(weather_file)
    df_park = pd.read_csv(park_file)

    merged = pd.merge(df_weather, df_park, on=["last_name, first_name", "team"], how="inner")

    # Calculate adj_woba_combined from confirmed columns
    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2

    merged.to_csv(output_file, index=False)
    print(f"✅ Merged {label} batter data written to {output_file}")
    return output_file

def commit_outputs(files):
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", *files], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: combined batter weather+park files"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed merged files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    files = []
    for label in ["home", "away"]:
        output_file = combine_adjustments(label)
        files.append(output_file)
    commit_outputs(files)

if __name__ == "__main__":
    main()
