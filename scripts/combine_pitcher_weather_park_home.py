import pandas as pd
from unidecode import unidecode
import subprocess

WEATHER_FILE = "data/adjusted/pitchers_home_weather.csv"
PARK_FILE = "data/adjusted/pitchers_home_park.csv"
TEAM_MASTER = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/adjusted/pitchers_home_weather_park.csv"
LOG_FILE = "log_pitchers_home_weather_park.txt"

def normalize_name(name):
    if pd.isna(name): return name
    return unidecode(name).strip()

def normalize_team(team, valid_teams):
    team = unidecode(str(team)).strip()
    matches = [vt for vt in valid_teams if vt.lower() == team.lower()]
    return matches[0] if matches else team

def merge_and_combine(weather_df, park_df, valid_teams):
    weather_df["last_name, first_name"] = weather_df["last_name, first_name"].apply(normalize_name)
    park_df["last_name, first_name"] = park_df["last_name, first_name"].apply(normalize_name)

    weather_df["home_team"] = weather_df["home_team"].apply(lambda x: normalize_team(x, valid_teams))
    park_df["home_team"] = park_df["home_team"].apply(lambda x: normalize_team(x, valid_teams))

    merged = pd.merge(
        weather_df,
        park_df,
        on=["last_name, first_name", "home_team"],
        how="inner"
    )

    if "adj_woba_weather" in merged.columns and "adj_woba_park" in merged.columns:
        merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2

    return merged

def reduce_columns(df):
    keep_cols = ["last_name, first_name", "home_team", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]
    return df[keep_cols]

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
        cleaned = reduce_columns(combined)
        cleaned.to_csv(OUTPUT_FILE, index=False)

        if cleaned.empty:
            log_entries.append("⚠️ WARNING: Merge returned 0 rows. Check for mismatched names or teams.")
        else:
            top5 = cleaned.sort_values(by="adj_woba_combined", ascending=False).head(5)
            log_entries.append("Top 5 Combined Pitchers by adj_woba_combined:")
            log_entries.append(top5.to_string(index=False))
    except Exception as e:
        log_entries.append(f"❌ Error during processing: {str(e)}")

    with open(LOG_FILE, "w") as log:
        for entry in log_entries:
            log.write(entry + "\n")

    try:
        subprocess.run(["git", "add", OUTPUT_FILE, LOG_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: Cleaned combine pitcher weather + park (home)"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push skipped or failed: {e}")

if __name__ == "__main__":
    main()
