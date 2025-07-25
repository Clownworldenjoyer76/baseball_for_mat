import pandas as pd

BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

OUTPUT_HOME = "data/adjusted/batters_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/batters_away_weather.csv"
LOG_HOME = "log_weather_home.txt"
LOG_AWAY = "log_weather_away.txt"

def apply_adjustment(df, side, weather_df):
    if "team" not in df.columns:
        raise ValueError(f"Missing 'team' column in batters_{side}.csv")
    if "venue" not in weather_df.columns:
        raise ValueError("Missing 'venue' column in weather_adjustments.csv")

    merged = df.merge(weather_df, left_on="team", right_on="venue", how="left")

    if "temperature" not in merged.columns:
        raise ValueError("Missing 'temperature' column after merge")

    # Adjust wOBA based on temperature
    merged["adj_woba_weather"] = merged["woba"]
    merged.loc[merged["temperature"] >= 85, "adj_woba_weather"] *= 1.03
    merged.loc[merged["temperature"] <= 50, "adj_woba_weather"] *= 0.97

    return merged

def write_log(df, path):
    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, row in top5.iterrows():
            f.write(f"{row['last_name, first_name']} - {row['team']} - {row['adj_woba_weather']:.3f}\n")

def main():
    batters_home = pd.read_csv(BATTERS_HOME)
    batters_away = pd.read_csv(BATTERS_AWAY)
    weather = pd.read_csv(WEATHER_FILE)

    adjusted_home = apply_adjustment(batters_home, "home", weather)
    adjusted_away = apply_adjustment(batters_away, "away", weather)

    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    write_log(adjusted_home, LOG_HOME)
    write_log(adjusted_away, LOG_AWAY)

if __name__ == "__main__":
    main()
