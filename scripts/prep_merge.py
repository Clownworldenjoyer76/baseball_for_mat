import pandas as pd
from pathlib import Path
import subprocess

# File paths
home_path = "data/adjusted/batters_home_weather_park.csv"
away_path = "data/adjusted/batters_away_weather_park.csv"
master_path = "data/Data/team_name_master.csv"

def normalize_team_column(df, team_map):
    df["team"] = df["team"].astype(str).str.strip().str.lower().map(team_map)
    return df

def normalize_and_write(path, team_map):
    df = pd.read_csv(path)
    df = normalize_team_column(df, team_map)
    df.to_csv(path, index=False)

def commit_files(paths):
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add"] + paths, check=True)
        subprocess.run(["git", "commit", "-m", "prep_merge: normalize team casing"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git operation failed: {e}")

def main():
    master = pd.read_csv(master_path)
    team_map = {team.lower(): team for team in master["team_name"]}

    normalize_and_write(home_path, team_map)
    normalize_and_write(away_path, team_map)

    commit_files([home_path, away_path])
    print("✅ Team column normalized and updated.")

if __name__ == "__main__":
    main()
