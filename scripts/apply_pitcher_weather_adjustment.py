import pandas as pd

HOME_PITCHERS = "data/adjusted/pitchers_home.csv"
AWAY_PITCHERS = "data/adjusted/pitchers_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

OUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUT_AWAY = "data/adjusted/pitchers_away_weather.csv"

LOG_HOME = "data/adjusted/log_pitchers_home_weather.txt"
LOG_AWAY = "data/adjusted/log_pitchers_away_weather.txt"

def apply_weather(df, weather_df, team_col, log_path):
    merged = pd.merge(df, weather_df, how="left", left_on=team_col, right_on=team_col)
    merged.to_csv(log_path.replace(".txt", ".csv"), index=False)

    # Minimal logging
    top5 = merged[["last_name, first_name", team_col, "temperature", "humidity", "adj_woba_weather"]].head(5)
    with open(log_path, "w") as f:
        f.write("Top 5 pitcher weather-adjusted rows:\n")
        f.write(top5.to_string(index=False))

    return merged

def main():
    weather_df = pd.read_csv(WEATHER_FILE)
    home = pd.read_csv(HOME_PITCHERS)
    away = pd.read_csv(AWAY_PITCHERS)

    if "home_team" not in weather_df.columns or "away_team" not in weather_df.columns:
        raise ValueError("weather_adjustments.csv missing home_team or away_team column")

    adjusted_home = apply_weather(home, weather_df, "home_team", LOG_HOME)
    adjusted_away = apply_weather(away, weather_df, "away_team", LOG_AWAY)

    adjusted_home.to_csv(OUT_HOME, index=False)
    adjusted_away.to_csv(OUT_AWAY, index=False)

    print(f"✅ Pitcher weather adjustment complete. Saved to:\n- {OUT_HOME}\n- {OUT_AWAY}")

if __name__ == "__main__":
    main()
