import pandas as pd
from pathlib import Path
from unidecode import unidecode

TEAM_MASTER = "data/Data/team_name_master.csv"
BATTERS_IN = "data/Data/batters.csv"
PITCHERS_IN = "data/Data/pitchers.csv"
BATTERS_OUT = "data/normalized/batters_normalized.csv"
PITCHERS_OUT = "data/normalized/pitchers_normalized.csv"

def normalize_name(name):
    if pd.isna(name):
        return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    parts = name.split()
    if len(parts) >= 2:
        normalized = f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    else:
        normalized = name.title()
    return normalized

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def normalize_dataframe(df, name_column, team_column, valid_teams):
    df[name_column] = df[name_column].apply(normalize_name)
    df[team_column] = df[team_column].apply(lambda x: normalize_team(x, valid_teams))
    return df

def main():
    Path("data/normalized").mkdir(parents=True, exist_ok=True)

    teams = pd.read_csv(TEAM_MASTER)["team_name"].dropna().unique().tolist()

    batters = pd.read_csv(BATTERS_IN)
    batters = normalize_dataframe(batters, name_column="last_name, first_name", team_column="team", valid_teams=teams)
    batters.to_csv(BATTERS_OUT, index=False)

    pitchers = pd.read_csv(PITCHERS_IN)
    pitchers = normalize_dataframe(pitchers, name_column="last_name, first_name", team_column="team", valid_teams=teams)
    pitchers.to_csv(PITCHERS_OUT, index=False)

    print("✅ Normalized batters and pitchers written to data/normalized/")

if __name__ == "__main__":
    main()
