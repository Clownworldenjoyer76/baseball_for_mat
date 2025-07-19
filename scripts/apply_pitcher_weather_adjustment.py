import pandas as pd

PITCHERS_HOME = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY = "data/adjusted/pitchers_away.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
WEATHER_FILE = "data/weather_adjustments.csv"
OUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUT_AWAY = "data/adjusted/pitchers_away_weather.csv"
LOG_HOME = "log_pitchers_home_weather.txt"
LOG_AWAY = "log_pitchers_away_weather.txt"

def apply_weather_adjustments(df, weather):
    # Use temp-based adjustment as example
    df["adj_woba"] = df["woba"]
    df.loc[weather["temperature"] > 85, "adj_woba"] *= 1.02
    df.loc[weather["temperature"] < 60, "adj_woba"] *= 0.98
    return df

def main():
    pitchers_home = pd.read_csv(PITCHERS_HOME)
    pitchers_away = pd.read_csv(PITCHERS_AWAY)
    stadiums = pd.read_csv(STADIUM_FILE)
    weather = pd.read_csv(WEATHER_FILE)

    # Merge home team with stadium file to get stadium name
    stadiums = stadiums[["team", "stadium"]]

    home_merged = pd.merge(pitchers_home, stadiums, left_on="home_team", right_on="team", how="left")
    away_merged = pd.merge(pitchers_away, stadiums, left_on="home_team", right_on="team", how="left")

    home_final = pd.merge(home_merged, weather, on="stadium", how="left")
    away_final = pd.merge(away_merged, weather, on="stadium", how="left")

    adjusted_home = apply_weather_adjustments(home_final, home_final)
    adjusted_away = apply_weather_adjustments(away_final, away_final)

    adjusted_home.to_csv(OUT_HOME, index=False)
    adjusted_away.to_csv(OUT_AWAY, index=False)

    with open(LOG_HOME, "w") as f:
        f.write(f"✅ Wrote {OUT_HOME} with {len(adjusted_home)} rows.\n")
    with open(LOG_AWAY, "w") as f:
        f.write(f"✅ Wrote {OUT_AWAY} with {len(adjusted_away)} rows.\n")

if __name__ == "__main__":
    main()
