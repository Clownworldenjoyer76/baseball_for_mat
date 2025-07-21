import pandas as pd
from unidecode import unidecode

# Input and output paths
WEATHER_IN = "data/adjusted/pitchers_away_weather.csv"
PARK_IN = "data/adjusted/pitchers_away_park.csv"
TEAM_MAP = "data/Data/team_name_master.csv"
WEATHER_OUT = "data/adjusted/pitchers_away_weather_cleaned.csv"
PARK_OUT = "data/adjusted/pitchers_away_park_cleaned.csv"
LOG_FILE = "log_preprocess_pitchers_away.txt"

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(str(name)).strip()
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return name.title()

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def clean_dataframe(df, team_col, valid_teams):
    df = df.copy()

    # Normalize name and team
    if "name" in df.columns:
        df["name"] = df["name"].apply(normalize_name)
    if team_col in df.columns:
        df[team_col] = df[team_col].apply(lambda x: normalize_team(x, valid_teams))

    # Drop junk columns
    cols_to_drop = [col for col in df.columns if col.endswith("_x") or col.endswith("_y") or col in [
        "venue", "city", "state", "timezone", "is_dome", "lat", "lon",
        "game_time", "temperature", "humidity", "wind_speed", "wind_direction",
        "condition", "pitcher_away_x", "pitcher_away_y", "adj_woba_weather"
    ]]
    df.drop(columns=cols_to_drop, errors="ignore", inplace=True)

    return df

def main():
    log = []
    try:
        weather = pd.read_csv(WEATHER_IN)
        park = pd.read_csv(PARK_IN)
        teams = pd.read_csv(TEAM_MAP)["team_name"].dropna().unique().tolist()

        weather_clean = clean_dataframe(weather, "away_team", teams)
        park_clean = clean_dataframe(park, "away_team", teams)

        weather_clean.to_csv(WEATHER_OUT, index=False)
        park_clean.to_csv(PARK_OUT, index=False)

        log.append("✅ Cleaned input files for pitcher weather+park merge.")
        log.append(f"✅ WEATHER columns: {weather_clean.columns.tolist()}")
        log.append(f"✅ PARK columns: {park_clean.columns.tolist()}")
    except Exception as e:
        log.append(f"❌ Error: {str(e)}")

    with open(LOG_FILE, "w") as f:
        f.write("\n".join(log))

if __name__ == "__main__":
    main()
