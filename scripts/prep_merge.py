import pandas as pd
from pathlib import Path
import subprocess

# File paths
home_path = "data/adjusted/batters_home_weather_park.csv"
away_path = "data/adjusted/batters_away_weather_park.csv"
master_path = "data/Data/team_name_master.csv"

# New files for name correction
pitchers_home_path = "data/adjusted/pitchers_home_weather_park.csv"
pitchers_away_path = "data/adjusted/pitchers_away_weather_park.csv"
games_path = "data/raw/todaysgames_normalized.csv"
normalized_pitchers_path = "data/cleaned/pitchers_normalized_cleaned.csv"

def normalize_team_column(df, team_map):
    df["team"] = df["team"].astype(str).strip().str.lower().map(team_map)
    return df

def normalize_and_write(path, team_map):
    df = pd.read_csv(path)
    df = normalize_team_column(df, team_map)
    df.to_csv(path, index=False)

def fix_pitcher_names_strict():
    normalized = pd.read_csv(normalized_pitchers_path)
    valid_names = normalized["last_name, first_name"].dropna().unique()

    name_map = {
        raw.strip().replace(",", "").upper(): valid
        for valid in valid_names
        for raw in [valid]
    }

    def map_name(name):
        if pd.isna(name): return name
        key = name.strip().rstrip(",").replace(" ,", ",").upper()
        return name_map.get(key, name)

    # Update home pitcher file
    ph = pd.read_csv(pitchers_home_path)
    if "last_name, first_name" in ph.columns:
        ph["last_name, first_name"] = ph["last_name, first_name"].apply(map_name)
    ph.to_csv(pitchers_home_path, index=False)

    # Update away pitcher file
    pa = pd.read_csv(pitchers_away_path)
    if "last_name, first_name" in pa.columns:
        pa["last_name, first_name"] = pa["last_name, first_name"].apply(map_name)
    pa.to_csv(pitchers_away_path, index=False)

    # Update todaysgames file
    games = pd.read_csv(games_path)
    if "pitcher_home" in games.columns:
        games["pitcher_home"] = games["pitcher_home"].apply(map_name)
    if "pitcher_away" in games.columns:
        games["pitcher_away"] = games["pitcher_away"].apply(map_name)
    games.to_csv(games_path, index=False)

def commit_files(paths):
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add"] + paths, check=True)
        subprocess.run(["git", "commit", "-m", "prep_merge: normalize team casing and enforce name match"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push complete.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git operation failed: {e}")

def main():
    master = pd.read_csv(master_path)
    team_map = {team.lower(): team for team in master["team_name"]}

    normalize_and_write(home_path, team_map)
    normalize_and_write(away_path, team_map)

    fix_pitcher_names_strict()

    commit_files([
        home_path,
        away_path,
        pitchers_home_path,
        pitchers_away_path,
        games_path
    ])
    print("✅ Team and name normalization complete.")

if __name__ == "__main__":
    main()
