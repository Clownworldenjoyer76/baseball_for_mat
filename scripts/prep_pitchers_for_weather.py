import pandas as pd
from unidecode import unidecode
from pathlib import Path

TEAM_MASTER = "data/Data/team_name_master.csv"
PITCHERS_HOME_IN = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_IN = "data/adjusted/pitchers_away.csv"
LOG_FILE = "log_pitcher_weather_input_cleanup.txt"

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    name = ','.join(part.strip() for part in name.split(','))
    return name.title()

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    for vt in valid_teams:
        if vt.lower() == team.lower():
            return vt
    return team

def clean_pitcher_df(df, side, valid_teams):
    # Normalize team and name columns
    df["team"] = df["team"].apply(lambda x: normalize_team(x, valid_teams))
    df["name"] = df["name"].apply(normalize_name)

    team_col = f"{side}_team"
    if team_col in df.columns:
        df[team_col] = df[team_col].apply(lambda x: normalize_team(x, valid_teams))

    # Drop rows where required fields are missing
    df = df.dropna(subset=["team", "name"])

    return df

def main():
    Path("data/adjusted").mkdir(parents=True, exist_ok=True)
    logs = []

    try:
        teams = pd.read_csv(TEAM_MASTER)["team_name"].dropna().tolist()
        home_df = pd.read_csv(PITCHERS_HOME_IN)
        away_df = pd.read_csv(PITCHERS_AWAY_IN)

        logs.append(f"🔍 Pre-clean HOME: {len(home_df)} rows")
        logs.append(f"🔍 Pre-clean AWAY: {len(away_df)} rows")

        home_df = clean_pitcher_df(home_df, "home", teams)
        away_df = clean_pitcher_df(away_df, "away", teams)

        logs.append(f"✅ Cleaned HOME: {len(home_df)} rows")
        logs.append(f"✅ Cleaned AWAY: {len(away_df)} rows")

        home_df.to_csv(PITCHERS_HOME_IN, index=False)
        away_df.to_csv(PITCHERS_AWAY_IN, index=False)

    except Exception as e:
        logs.append(f"❌ Error: {str(e)}")

    with open(LOG_FILE, "w") as log:
        for line in logs:
            log.write(line + "\n")

if __name__ == "__main__":
    main()
