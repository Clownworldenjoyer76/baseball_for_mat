import pandas as pd

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"

def load_team_map():
    df = pd.read_csv(TEAM_MAP_FILE)
    team_dict = dict(zip(df["name"].str.strip().str.lower(), df["team"].str.strip()))
    return team_dict

def main():
    team_dict = load_team_map()

    tp_df = pd.read_csv(PITCHERS_FILE)
    sm_df = pd.read_csv(STADIUM_FILE)

    tp_df["home_team"] = tp_df["home_team"].str.strip().str.lower().map(team_dict)
    sm_df["home_team"] = sm_df["home_team"].str.strip().str.lower().map(team_dict)

    merged = pd.merge(tp_df, sm_df, on="home_team", how="inner")

    if not merged.empty:
        final = merged[[
            "home_team", "game_time", "venue", "city", "state", 
            "timezone", "is_dome", "latitude", "longitude"
        ]]
        final.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Created {OUTPUT_FILE}")
    else:
        print("⚠️ No matching data found. Output file not created.")

if __name__ == "__main__":
    main()
