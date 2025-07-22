import pandas as pd
import os

PITCHERS_HOME = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY = "data/adjusted/pitchers_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

OUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUT_AWAY = "data/adjusted/pitchers_away_weather.csv"

LOG_HOME = "log_pitchers_home_weather.txt"
LOG_AWAY = "log_pitchers_away_weather.txt"

def load_data():
    home = pd.read_csv(PITCHERS_HOME)
    away = pd.read_csv(PITCHERS_AWAY)
    weather = pd.read_csv(WEATHER_FILE)
    return home, away, weather

def apply_weather(df, weather_df, side, log_file):
    if df.empty:
        print(f"⚠️ No {side} pitchers to process.")
        df.to_csv(OUT_HOME if side == "home" else OUT_AWAY, index=False)
        return

    df = df.copy()
    merged = pd.merge(df, weather_df, how="left", on="team", suffixes=('', '_weather'))
    
    if "temperature" in merged.columns:
        merged["adj_woba_weather"] = merged["woba"] * (1 + ((merged["temperature"] - 70) * 0.005).fillna(0))
    else:
        merged["adj_woba_weather"] = merged["woba"]

    # Save result
    out_path = OUT_HOME if side == "home" else OUT_AWAY
    merged.to_csv(out_path, index=False)

    # Save top 5 to log
    top5 = merged.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(log_file, "w") as f:
        f.write(f"Top 5 {side} pitchers (weather-adjusted wOBA):\n")
        for _, row in top5.iterrows():
            f.write(f"{row.get('name', 'Unknown')}: {row.get('adj_woba_weather', 0):.3f}\n")

    print(f"✅ Wrote {len(merged)} rows to {out_path}")
    print(f"📝 Log: {log_file}")

def main():
    home, away, weather = load_data()
    apply_weather(home, weather, "home", LOG_HOME)
    apply_weather(away, weather, "away", LOG_AWAY)

if __name__ == "__main__":
    main()
