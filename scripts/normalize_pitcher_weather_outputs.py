import pandas as pd
from pathlib import Path
from unidecode import unidecode

TEAM_MASTER = "data/Data/team_name_master.csv"
PITCHERS_HOME = "data/adjusted/pitchers_home_weather.csv"
PITCHERS_AWAY = "data/adjusted/pitchers_away_weather.csv"

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return name.title()

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    for valid in valid_teams:
        if valid.lower() == team.lower():
            return valid
    return team

def normalize_file(path, valid_teams):
    df = pd.read_csv(path)
    if "name" in df.columns:
        df["name"] = df["name"].apply(normalize_name)
    if "team" in df.columns:
        df["team"] = df["team"].apply(lambda x: normalize_team(x, valid_teams))
    df.to_csv(path, index=False)
    print(f"✅ Normalized: {path}")

def main():
    teams = pd.read_csv(TEAM_MASTER)["team_name"].dropna().unique().tolist()
    normalize_file(PITCHERS_HOME, teams)
    normalize_file(PITCHERS_AWAY, teams)

if __name__ == "__main__":
    main()
