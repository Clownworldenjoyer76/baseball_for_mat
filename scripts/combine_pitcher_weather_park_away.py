import pandas as pd
import os

WEATHER_FILE = "data/adjusted/pitchers_away_weather.csv"
PARK_FILE = "data/adjusted/pitchers_away_park.csv"
OUTPUT_FILE = "data/adjusted/pitchers_away_adjusted.csv"
LOG_FILE = "data/adjusted/log_combined_pitchers_away.txt"

def normalize_team(name, valid_teams):
    name = str(name).strip().title()
    if name in valid_teams:
        return name
    for team in valid_teams:
        if name.lower() in team.lower() or team.lower() in name.lower():
            return team
    return name

def merge_and_combine(weather_df, park_df, valid_teams):
    # Handle team normalization fallback
    if "away_team_x" in weather_df.columns:
        weather_df["away_team"] = weather_df["away_team_x"].apply(lambda x: normalize_team(x, valid_teams))
    elif "away_team" in weather_df.columns:
        weather_df["away_team"] = weather_df["away_team"].apply(lambda x: normalize_team(x, valid_teams))

    park_df["away_team"] = park_df["away_team"].apply(lambda x: normalize_team(x, valid_teams))

    # Merge on normalized away_team and player name
    merged_df = pd.merge(weather_df, park_df, on=["last_name, first_name", "away_team"], suffixes=("_weather", "_park"))

    return merged_df

def write_log(df, log_path):
    top = df.sort_values(by="adj_woba_park", ascending=False).head(5)
    with open(log_path, "w") as f:
        f.write("Top 5 Away Pitchers by Park-Adjusted wOBA:\n")
        for _, row in top.iterrows():
            f.write(f"{row['last_name, first_name']} - {row['team']} - {row['adj_woba_park']:.3f}\n")

def main():
    weather_df = pd.read_csv(WEATHER_FILE)
    park_df = pd.read_csv(PARK_FILE)

    valid_teams = sorted(set(park_df["away_team"].dropna().unique()) | set(weather_df.get("away_team", pd.Series()).dropna().unique()))

    merged_df = merge_and_combine(weather_df, park_df, valid_teams)
    merged_df.to_csv(OUTPUT_FILE, index=False)
    write_log(merged_df, LOG_FILE)
    print(f"✅ Saved combined file to: {OUTPUT_FILE}")
    print(f"📝 Log written to: {LOG_FILE}")

if __name__ == "__main__":
    main()
