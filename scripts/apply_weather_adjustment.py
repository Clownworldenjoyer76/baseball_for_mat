import pandas as pd

# File paths
BATTERS_HOME_FILE = "data/adjusted/batters_home.csv"
BATTERS_AWAY_FILE = "data/adjusted/batters_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv"
OUTPUT_HOME = "data/adjusted/batters_home_weather_park.csv"
OUTPUT_AWAY = "data/adjusted/batters_away_weather.csv"
LOG_HOME = "log_weather_home.txt"
LOG_AWAY = "log_weather_away.txt"

# Adjustment logic
def apply_weather_adjustments(batters_df, weather_df):
    merged = pd.merge(
        batters_df,
        weather_df,
        left_on="home_team",
        right_on="stadium",
        how="left"
    )
    merged["adj_woba_weather"] = merged["woba"] * merged["temperature"].apply(
        lambda x: 1.02 if x > 85 else 0.98 if x < 60 else 1.0
    )
    return merged

def main():
    # Load files
    batters_home = pd.read_csv(BATTERS_HOME_FILE)
    batters_away = pd.read_csv(BATTERS_AWAY_FILE)
    weather = pd.read_csv(WEATHER_FILE)

    # Apply adjustments
    adjusted_home = apply_weather_adjustments(batters_home, weather)
    adjusted_away = apply_weather_adjustments(batters_away, weather)

    # Save adjusted files
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    # Top 5 logs
    top5_home = adjusted_home.sort_values(by="adj_woba_weather", ascending=False).head(5)
    top5_away = adjusted_away.sort_values(by="adj_woba_weather", ascending=False).head(5)

    with open(LOG_HOME, "w") as f:
        f.write("Top 5 Home Batters by adj_woba_weather:\n")
        f.write(top5_home[["name", "team", "woba", "temperature", "adj_woba_weather"]].to_string(index=False))

    with open(LOG_AWAY, "w") as f:
        f.write("Top 5 Away Batters by adj_woba_weather:\n")
        f.write(top5_away[["name", "team", "woba", "temperature", "adj_woba_weather"]].to_string(index=False))

if __name__ == "__main__":
    main()
