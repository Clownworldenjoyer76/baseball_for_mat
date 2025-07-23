
import pandas as pd

PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
WEATHER_TEAMS_FILE = "data/weather_teams.csv"

OUTPUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_weather.csv"
LOG_HOME = "data/adjusted/log_pitchers_home_weather.txt"
LOG_AWAY = "data/adjusted/log_pitchers_away_weather.txt"

def apply_weather_adjustment(pitchers_df, weather_df, team_col):
    merged = pd.merge(pitchers_df, weather_df, left_on=team_col, right_on=team_col, how="left")
    merged["adj_woba_weather"] = merged["woba"] * (merged["temperature"] / 75)
    return merged

def main():
    home_pitchers = pd.read_csv(PITCHERS_HOME_FILE)
    away_pitchers = pd.read_csv(PITCHERS_AWAY_FILE)
    weather_df = pd.read_csv(WEATHER_TEAMS_FILE)

    home_weather = apply_weather_adjustment(home_pitchers, weather_df, "home_team")
    away_weather = apply_weather_adjustment(away_pitchers, weather_df, "away_team")

    home_weather.to_csv(OUTPUT_HOME, index=False)
    away_weather.to_csv(OUTPUT_AWAY, index=False)

    with open(LOG_HOME, "w") as f:
        f.write(home_weather[["last_name, first_name", "team", "woba", "temperature", "adj_woba_weather"]]
                .sort_values("adj_woba_weather", ascending=False).head(5).to_string(index=False))

    with open(LOG_AWAY, "w") as f:
        f.write(away_weather[["last_name, first_name", "team", "woba", "temperature", "adj_woba_weather"]]
                .sort_values("adj_woba_weather", ascending=False).head(5).to_string(index=False))

if __name__ == "__main__":
    main()
