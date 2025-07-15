import pandas as pd

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"

def load_team_map():
    df = pd.read_csv(TEAM_MAP_FILE)
    team_dict = dict(zip(df["name"].str.strip().str.lower(), df["team"].str.strip()))
    return team_dict

def apply_mapping(df, column, team_dict):
    original = df[column].astype(str).str.strip()
    lookup = original.str.lower()
    mapped = lookup.map(team_dict)
    df[column] = mapped.fillna(original).str.strip()
    return df

def main():
    team_dict = load_team_map()

    # Apply to todaysgames_normalized
    games_df = pd.read_csv(GAMES_FILE)
    if 'home_team' in games_df.columns:
        games_df = apply_mapping(games_df, "home_team", team_dict)
    if 'away_team' in games_df.columns:
        games_df = apply_mapping(games_df, "away_team", team_dict)
    games_df.to_csv(GAMES_FILE, index=False)
    print(f"✅ Updated: {GAMES_FILE}")

    # Apply to stadium_metadata
    sm_df = pd.read_csv(STADIUM_FILE)
    if 'home_team' in sm_df.columns:
        sm_df = apply_mapping(sm_df, "home_team", team_dict)
        sm_df.to_csv(STADIUM_FILE, index=False)
        print(f"✅ Updated: {STADIUM_FILE}")

if __name__ == "__main__":
    main()
