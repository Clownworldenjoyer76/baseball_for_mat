import pandas as pd
import os

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def load_team_map():
    df = pd.read_csv(TEAM_MAP_FILE)
    return dict(zip(df["name"].str.strip().str.lower(), df["team"].str.strip()))

def standardize_team_names(df, column, team_dict):
    df[column] = df[column].str.strip().str.lower().map(team_dict).fillna(df[column])
    return df

def main():
    team_map = load_team_map()
    tp = pd.read_csv(PITCHERS_FILE)
    sm = pd.read_csv(STADIUM_FILE)

    if "home_team" not in tp.columns or "home_team" not in sm.columns:
        print("Missing 'home_team' column in one of the input files.")
        return

    tp = standardize_team_names(tp, "home_team", team_map)
    sm = standardize_team_names(sm, "home_team", team_map)

    sm = sm[[
        "home_team", "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude"
    ]]

    merged = tp.merge(sm, on="home_team", how="inner")
    output = merged[[
        "home_team", "game_time", "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude"
    ]]

    os.makedirs("data", exist_ok=True)
    output.to_csv(OUTPUT_FILE, index=False)

    with open(SUMMARY_FILE, "w") as f:
        f.write(f"✅ Created: {OUTPUT_FILE}\n")
        f.write(f"✅ Summary written to: {SUMMARY_FILE}\n")
        f.write(f"✅ Total records written: {len(output)}\n")

    print(f"✅ Created: {OUTPUT_FILE}")
    print(f"✅ Summary written to: {SUMMARY_FILE}")

if __name__ == "__main__":
    main()