
import pandas as pd

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"

def main():
    # Load data
    team_map = pd.read_csv(TEAM_MAP_FILE)
    team_dict = dict(zip(team_map["name"].str.strip().str.lower(), team_map["team"].str.strip()))

    pitchers_df = pd.read_csv(PITCHERS_FILE)
    stadium_df = pd.read_csv(STADIUM_FILE)

    # Standardize team names
    pitchers_df["home_team"] = pitchers_df["home_team"].str.strip().str.lower().map(team_dict)
    stadium_df["home_team"] = stadium_df["home_team"].str.strip().str.lower().map(team_dict)

    # Drop missing mappings
    pitchers_df = pitchers_df.dropna(subset=["home_team"])
    stadium_df = stadium_df.dropna(subset=["home_team"])

    # Merge on standardized home_team
    merged_df = pd.merge(pitchers_df[["home_team", "game_time"]], stadium_df, on="home_team", how="inner")

    # Select required columns
    merged_df = merged_df[[
        "home_team", "game_time", "venue", "city", "state",
        "timezone", "is_dome", "latitude", "longitude"
    ]]

    # Save to output
    merged_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Weather input CSV created at {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
