# scripts/update_pitcher_team_names.py

import pandas as pd
import os

TEAM_MAP_FILE = "data/Data/team_name_master.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"

def update_team_names(pitchers_file):
    if not os.path.exists(pitchers_file):
        print(f"⚠️ File not found: {pitchers_file}")
        return

    df = pd.read_csv(pitchers_file)
    team_map_df = pd.read_csv(TEAM_MAP_FILE)

    if "team_code" not in team_map_df.columns or "team_name" not in team_map_df.columns:
        raise ValueError("❌ team_name_master.csv must contain 'team_code' and 'team_name' columns.")

    team_map = dict(zip(team_map_df["team_code"], team_map_df["team_name"]))

    if "team" not in df.columns:
        raise ValueError(f"❌ Missing 'team' column in {pitchers_file}.")

    original_team_values = df["team"].unique()
    df["team"] = df["team"].map(team_map).fillna(df["team"])

    updated_team_values = df["team"].unique()
    print(f"🔁 Updated team names in {pitchers_file}")
    print(f"🔎 Original teams: {original_team_values}")
    print(f"✅ New teams: {updated_team_values}")

    df.to_csv(pitchers_file, index=False)

def main():
    update_team_names(PITCHERS_HOME_FILE)
    update_team_names(PITCHERS_AWAY_FILE)

if __name__ == "__main__":
    main()
