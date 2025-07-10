import pandas as pd

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"

def load_team_map():
    df = pd.read_csv(TEAM_MAP_FILE)
    return dict(zip(df["team"].str.strip(), df["mapped"].str.strip()))

def standardize_names(df, column, team_map):
    df[column] = df[column].str.strip().map(team_map).fillna(df[column])
    return df

def main():
    team_map = load_team_map()

    # Standardize todays_pitchers.csv
    pitchers_df = pd.read_csv(PITCHERS_FILE)
    pitchers_df = standardize_names(pitchers_df, "home_team", team_map)
    pitchers_df = standardize_names(pitchers_df, "away_team", team_map)
    pitchers_df.to_csv(PITCHERS_FILE, index=False)

    # Standardize stadium_metadata.csv
    stadium_df = pd.read_csv(STADIUM_FILE)
    stadium_df = standardize_names(stadium_df, "home_team", team_map)
    stadium_df.to_csv(STADIUM_FILE, index=False)

    print("✅ Team names standardized successfully.")

if __name__ == "__main__":
    main()