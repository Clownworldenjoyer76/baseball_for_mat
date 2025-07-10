import pandas as pd

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"

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

    tp = standardize_team_names(tp, "home_team", team_map)
    sm = standardize_team_names(sm, "home_team", team_map)

    # Filter stadium metadata to only include teams present in todays_pitchers
    sm_filtered = sm[sm["home_team"].isin(tp["home_team"])]

    # Join only needed columns, no merge of game_time
    result = pd.DataFrame()
    result["home_team"] = tp["home_team"]
    result["game_time"] = tp["game_time"]

    sm_filtered = sm_filtered.set_index("home_team").reindex(tp["home_team"]).reset_index()

    for col in ["venue", "city", "state", "timezone", "is_dome", "latitude", "longitude"]:
        result[col] = sm_filtered[col]

    result.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Created: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
