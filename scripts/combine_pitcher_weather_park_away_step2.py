
import pandas as pd

WEATHER_FILE = "data/adjusted/pitchers_away_weather.csv"
PARK_FILE = "data/adjusted/pitchers_away_park.csv"
OUTPUT_FILE = "data/adjusted/pitchers_away_weather_park.csv"
LOG_FILE = "log_pitchers_away_weather_park.txt"

def merge_and_combine(weather_df, park_df):
    merged = pd.merge(weather_df, park_df, on=["name", "away_team"], how="inner", suffixes=("_weather", "_park"))

    if "adj_woba_weather" in merged.columns and "adj_woba_park" in merged.columns:
        merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2

    return merged

def main():
    log_entries = []

    try:
        weather = pd.read_csv(WEATHER_FILE)
        park = pd.read_csv(PARK_FILE)

        combined = merge_and_combine(weather, park)
        combined.to_csv(OUTPUT_FILE, index=False)

        top5 = combined[["name", "away_team", "adj_woba_combined"]].sort_values(by="adj_woba_combined", ascending=False).head(5)
        log_entries.append("Top 5 Combined Away Pitchers by adj_woba_combined:")
        log_entries.append(top5.to_string(index=False))

    except Exception as e:
        log_entries.append(f"Error during processing: {str(e)}")

    with open(LOG_FILE, "w") as log:
        for entry in log_entries:
            log.write(entry + "\n")

if __name__ == "__main__":
    main()
