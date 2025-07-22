import pandas as pd
from unidecode import unidecode
import subprocess

WEATHER_FILE = "data/adjusted/pitchers_away_weather.csv"
PARK_FILE = "data/adjusted/pitchers_away_park.csv"
TEAM_MASTER = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/adjusted/pitchers_away_weather_park.csv"
LOG_FILE = "log_pitchers_away_weather_park.txt"

def normalize_name(name):
    if pd.isna(name):
        return name
    return unidecode(name).strip()

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def merge_and_combine(weather_df, park_df, valid_teams):
    weather_df["last_name, first_name"] = weather_df["last_name, first_name"].apply(normalize_name)
    park_df["last_name, first_name"] = park_df["last_name, first_name"].apply(normalize_name)

    weather_df["away_team"] = weather_df["away_team_x"].apply(lambda x: normalize_team(x, valid_teams))
    park_df["away_team"] = park_df["away_team"].apply(lambda x: normalize_team(x, valid_teams))

    merged = pd.merge(
        weather_df,
        park_df,
        on=["last_name, first_name", "away_team"],
        suffixes=("_weather", "_park")
    )

    if "adj_woba_weather" in merged.columns and "adj_woba_park" in merged.columns:
        merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2
    else:
        merged["adj_woba_combined"] = 0  # Fallback

    return merged

def write_top_5_log(df, path):
    top_5 = df.sort_values(by="adj_woba_combined", ascending=False).head(5)
    with open(path, "w") as f:
        f.write("Top 5 Pitchers - Away (Adj wOBA Combined)\n")
        for _, row in top_5.iterrows():
            name = row["last_name, first_name"]
            team = row["away_team"]
            woba = row["adj_woba_combined"]
            f.write(f"{name} ({team}): {woba:.3f}\n")

def main():
    weather_df = pd.read_csv(WEATHER_FILE)
    park_df = pd.read_csv(PARK_FILE)
    team_master = pd.read_csv(TEAM_MASTER)
    valid_teams = team_master["team_name"].tolist()

    merged_df = merge_and_combine(weather_df, park_df, valid_teams)
    merged_df.to_csv(OUTPUT_FILE, index=False)
    write_top_5_log(merged_df, LOG_FILE)

    subprocess.run(["git", "add", OUTPUT_FILE, LOG_FILE])
    subprocess.run(["git", "commit", "-m", "Auto-commit: Combined away pitcher adjustments"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
