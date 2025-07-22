import pandas as pd
from unidecode import unidecode
import subprocess

WEATHER_FILE = "data/adjusted/pitchers_away_weather.csv"
PARK_FILE = "data/adjusted/pitchers_away_park.csv"
TEAM_MASTER = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/adjusted/pitchers_away_weather_park.csv"
LOG_FILE = "log_pitchers_away_weather_park.txt"

def normalize_name(name):
    if pd.isna(name): return name
    return unidecode(name).strip()

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def merge_and_combine(weather_df, park_df, valid_teams):
    weather_df["last_name, first_name"] = weather_df["last_name, first_name"].apply(normalize_name)
    park_df["last_name, first_name"] = park_df["last_name, first_name"].apply(normalize_name)

    weather_df["team"] = weather_df["team"].apply(lambda x: normalize_team(x, valid_teams))
    park_df["team"] = park_df["team"].apply(lambda x: normalize_team(x, valid_teams))

    merged = pd.merge(
        weather_df,
        park_df,
        on=["last_name, first_name", "team"]
    )
    return merged

def main():
    weather_df = pd.read_csv(WEATHER_FILE)
    park_df = pd.read_csv(PARK_FILE)
    teams_df = pd.read_csv(TEAM_MASTER)
    valid_teams = teams_df["team_name"].dropna().unique().tolist()

    merged_df = merge_and_combine(weather_df, park_df, valid_teams)
    merged_df.to_csv(OUTPUT_FILE, index=False)

    top_5 = merged_df.sort_values(by="adj_woba_combined", ascending=False).head(5)
    with open(LOG_FILE, "w") as f:
        f.write(top_5.to_string(index=False))

    subprocess.run(["git", "add", OUTPUT_FILE, LOG_FILE])
    subprocess.run(["git", "commit", "-m", "✅ Combined away pitcher adjustments"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
