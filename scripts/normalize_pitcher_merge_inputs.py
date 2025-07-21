import pandas as pd
from unidecode import unidecode
import os

TEAM_MAP_FILE = "data/Data/team_name_master.csv"
PITCHER_REF_FILE = "data/Data/pitchers.csv"

FILES = {
    "data/adjusted/pitchers_home_weather.csv": "home_team",
    "data/adjusted/pitchers_home_park.csv": "home_team",
    "data/adjusted/pitchers_away_weather.csv": "away_team",
    "data/adjusted/pitchers_away_park.csv": "away_team",
}

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name).strip().lower()
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return name.title()

def normalize_team(team, team_map):
    if pd.isna(team): return team
    team = unidecode(str(team)).strip().lower()
    for valid in team_map:
        if team == valid.lower():
            return valid
    return team

def main():
    try:
        # Load team name reference
        team_map = pd.read_csv(TEAM_MAP_FILE)["team_name"].dropna().tolist()

        # Load pitcher name reference
        ref_pitchers = pd.read_csv(PITCHER_REF_FILE)
        ref_pitchers["normalized_name"] = ref_pitchers["last_name, first_name"].apply(normalize_name)
        ref_set = set(ref_pitchers["normalized_name"])

        for path, team_col in FILES.items():
            if not os.path.exists(path):
                print(f"❌ Missing file: {path}")
                continue

            df = pd.read_csv(path)

            # Drop old 'name' column
            if "name" in df.columns:
                df = df.drop(columns=["name"])

            # Normalize pitcher names
            if "last_name, first_name" in df.columns:
                df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
                df = df[df["last_name, first_name"].isin(ref_set)]

            # Normalize team names if applicable
            if team_col in df.columns:
                df[team_col] = df[team_col].apply(lambda x: normalize_team(x, team_map))

            # Normalize 'team' column if it exists
            if "team" in df.columns:
                df["team"] = df["team"].apply(lambda x: normalize_team(x, team_map))

            # Overwrite file
            df.to_csv(path, index=False)
            print(f"✅ Normalized and saved: {path}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
