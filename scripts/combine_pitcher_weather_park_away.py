import pandas as pd
from unidecode import unidecode
import subprocess
import os

WEATHER_FILE = "data/adjusted/pitchers_away_weather.csv"
PARK_FILE = "data/adjusted/pitchers_away_park.csv"
TEAM_MASTER = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/adjusted/pitchers_away_weather_park.csv"
LOG_FILE = "log_pitchers_away_weather_park.txt"

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    parts = name.split()
    if len(parts) >= 2:
        normalized = f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    else:
        normalized = name.title()
    return normalized

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def merge_and_combine(weather_df, park_df, valid_teams):
    weather_df["name"] = weather_df["name"].apply(normalize_name)
    weather_df["away_team"] = weather_df["away_team"].apply(lambda x: normalize_team(x, valid_teams))
    park_df["name"] = park_df["name"].apply(normalize_name)
    park_df["away_team"] = park_df["away_team"].apply(lambda x: normalize_team(x, valid_teams))

    merged = pd.merge(weather_df, park_df, on=["name", "away_team"], how="inner", suffixes=("_weather", "_park"))

    if "adj_woba_weather" in merged.columns and "adj_woba_park" in merged.columns:
        merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2

    return merged

def main():
    log_entries = []

    try:
        weather = pd.read_csv(WEATHER_FILE)
        park = pd.read_csv(PARK_FILE)
        teams = pd.read_csv(TEAM_MASTER)["team_name"].dropna().unique().tolist()
    except Exception as e:
        with open(LOG_FILE, "w") as log:
            log.write(f"❌ Failed to read input files: {e}\n")
        return

    try:
        combined = merge_and_combine(weather, park, teams)
        combined.to_csv(OUTPUT_FILE, index=False)

        if combined.empty:
            log_entries.append("⚠️ WARNING: Merge returned 0 rows. Check for mismatched names or teams.")
        else:
            top5 = combined[["name", "away_team", "adj_woba_combined"]].sort_values(by="adj_woba_combined", ascending=False).head(5)
            log_entries.append("Top 5 Combined Pitchers by adj_woba_combined:")
            log_entries.append(top5.to_string(index=False))
    except Exception as e:
        log_entries.append(f"❌ Error during processing: {str(e)}")

    with open(LOG_FILE, "w") as log:
        for entry in log_entries:
            log.write(entry + "\n")

    # Force Git to detect file change
    with open(OUTPUT_FILE, "a") as f:
        f.write(" ")
        f.flush()
        os.fsync(f.fileno())

    subprocess.run(["git", "add", OUTPUT_FILE, LOG_FILE], check=True)
    subprocess.run(["git", "commit", "-m", "Auto-commit: Combined pitcher weather + park (away)"], check=True)
    subprocess.run(["git", "push"], check=True)

if __name__ == "__main__":
    main()
