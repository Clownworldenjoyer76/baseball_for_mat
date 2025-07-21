import pandas as pd
from pathlib import Path
from unidecode import unidecode

TEAM_MASTER = "data/Data/team_name_master.csv"
IN_HOME = "data/adjusted/pitchers_home_weather.csv"
IN_AWAY = "data/adjusted/pitchers_away_weather.csv"
OUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUT_AWAY = "data/adjusted/pitchers_away_weather.csv"

USELESS_COLS = [
    "away_team", "pitcher_home_y", "pitcher_away", "venue", "city", "state", "timezone",
    "is_dome", "lat", "lon", "game_time", "temperature", "humidity", "wind_speed",
    "wind_direction", "condition"
]

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name).strip().lower()
    name = ' '.join(name.split())  # collapse whitespace
    name = name.replace(",,", ",")  # fix double commas
    parts = name.split(',')
    if len(parts) == 2:
        return f"{parts[0].title()}, {parts[1].title()}"
    name_parts = name.split()
    if len(name_parts) >= 2:
        return f"{name_parts[-1].title()}, {' '.join(name_parts[:-1]).title()}"
    return name.title()

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def clean_dataframe(df, team_col, pitcher_col, valid_teams):
    # Normalize key columns
    df["name"] = df["name"].apply(normalize_name)
    df["team"] = df["team"].apply(lambda x: normalize_team(x, valid_teams))
    if team_col in df.columns:
        df[team_col] = df[team_col].apply(lambda x: normalize_team(x, valid_teams))

    # Fix bad merge col
    if pitcher_col in df.columns:
        df.rename(columns={pitcher_col: "pitcher_home"}, inplace=True)
        df["pitcher_home"] = df["pitcher_home"].apply(normalize_name)

    # Drop irrelevant cols if present
    df.drop(columns=[c for c in USELESS_COLS if c in df.columns], inplace=True)

    # Repair adj_woba_weather if empty
    if "adj_woba_weather" in df.columns and "woba" in df.columns:
        df["adj_woba_weather"] = df["adj_woba_weather"].fillna(df["woba"])

    return df

def main():
    Path("data/adjusted").mkdir(parents=True, exist_ok=True)

    teams = pd.read_csv(TEAM_MASTER)["team_name"].dropna().unique().tolist()

    home_df = pd.read_csv(IN_HOME)
    away_df = pd.read_csv(IN_AWAY)

    home_df = clean_dataframe(home_df, "home_team", "pitcher_home_x", teams)
    away_df = clean_dataframe(away_df, "away_team", "pitcher_home_x", teams)

    home_df.to_csv(OUT_HOME, index=False)
    away_df.to_csv(OUT_AWAY, index=False)

    print("✅ Normalized pitcher weather files saved.")

if __name__ == "__main__":
    main()
