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

    weather_df["away_team"] = weather_df["away_team_x"].apply(lambda x: normalize_team(x, valid_teams))
    park_df["away_team"] = park_df["away_team"].apply(lambda x: normalize_team(x, valid_teams))

    merged = pd.merge(
        weather_df,
        park_df,
        on=["last_name, first_name", "away_team"]
