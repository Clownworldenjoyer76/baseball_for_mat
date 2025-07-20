import pandas as pd

PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
STADIUM_METADATA_FILE = "data/Data/stadium_metadata.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY_FILE = "data/adjusted/pitchers_away_weather.csv"

LOG_FILE = "log_pitchers_weather.txt"

def apply_weather_adjustments(pitchers_df, weather_df, side="home"):
    col_team = f"{side}_team"
    weather_df = weather_df.rename(columns={"stadium": "venue"})
    weather_df = weather_df.drop_duplicates(subset=["venue"])

    merged = pd.merge(pitchers_df, weather_df, left_on=col_team, right_on="venue", how="left")
    
    if "temperature" in merged.columns:
        merged["adj_woba_weather"] = merged["woba"] * (
            1 + ((merged["temperature"] - 70) * 0.002).fillna(0)
        )
    else:
        merged["adj_woba_weather"] = merged["woba"]

    return merged

def main():
    try:
        pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)
        pitchers_away = pd.read_csv(PITCHERS_AWAY_FILE)
        stadiums = pd.read_csv(STADIUM_METADATA_FILE)
        weather = pd.read_csv(WEATHER_FILE)
    except Exception as e:
        with open(LOG_FILE, "w") as log:
            log.write(f"❌ Failed to read input files: {e}\n")
        return

    try:
        stadiums = stadiums[["team_name", "venue"]]
        stadiums = stadiums.rename(columns={"team_name": "team", "venue": "stadium"})
    except Exception as e:
        with open(LOG_FILE, "w") as log:
            log.write(f"❌ Failed to process stadiums file: {e}\n")
        return

    try:
        adjusted_home = apply_weather_adjustments(pitchers_home, weather, side="home")
        adjusted_away = apply_weather_adjustments(pitchers_away, weather, side="away")

        adjusted_home.to_csv(OUTPUT_HOME_FILE, index=False)
        adjusted_away.to_csv(OUTPUT_AWAY_FILE, index=False)

        with open(LOG_FILE, "w") as log:
            log.write(f"✅ Wrote {OUTPUT_HOME_FILE} with {len(adjusted_home)} rows.\n")
            log.write(f"✅ Wrote {OUTPUT_AWAY_FILE} with {len(adjusted_away)} rows.\n")
    except Exception as e:
        with open(LOG_FILE, "w") as log:
            log.write(f"❌ Error during processing: {e}\n")

if __name__ == "__main__":
    main()
from unidecode import unidecode

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    name = ','.join(part.strip() for part in name.split(','))
    return name.title()


