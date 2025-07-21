import pandas as pd
from pathlib import Path
from unidecode import unidecode
import subprocess

TEAM_MASTER = "data/Data/team_name_master.csv"
PITCHERS_HOME_IN = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_IN = "data/adjusted/pitchers_away.csv"
PITCHERS_HOME_OUT = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_OUT = "data/adjusted/pitchers_away.csv"

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    name = ','.join(part.strip() for part in name.split(','))
    return name.title()

def normalize_team(team, valid_teams):
    if pd.isna(team): return team
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def normalize_file(path_in, path_out, valid_teams):
    df = pd.read_csv(path_in)
    if "name" in df.columns:
        df["name"] = df["name"].apply(normalize_name)
    if "team" in df.columns:
        df["team"] = df["team"].apply(lambda x: normalize_team(x, valid_teams))
    if "home_team" in df.columns:
        df["home_team"] = df["home_team"].apply(lambda x: normalize_team(x, valid_teams))
    if "away_team" in df.columns:
        df["away_team"] = df["away_team"].apply(lambda x: normalize_team(x, valid_teams))
    df.to_csv(path_out, index=False)

def commit_outputs():
    subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
    subprocess.run(["git", "add", PITCHERS_HOME_OUT, PITCHERS_AWAY_OUT], check=True)
    subprocess.run(["git", "commit", "--allow-empty", "-m", "Normalize pitchers before weather adjustment"], check=True)
    subprocess.run(["git", "push"], check=True)
    print("✅ Forced commit and push to repo.")

def main():
    teams = pd.read_csv(TEAM_MASTER)["team_name"].dropna().unique().tolist()
    normalize_file(PITCHERS_HOME_IN, PITCHERS_HOME_OUT, teams)
    normalize_file(PITCHERS_AWAY_IN, PITCHERS_AWAY_OUT, teams)
    commit_outputs()

if __name__ == "__main__":
    main()
