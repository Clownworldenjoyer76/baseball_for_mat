import pandas as pd

HOME_FILE = "data/adjusted/pitchers_home.csv"
AWAY_FILE = "data/adjusted/pitchers_away.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"

def update_team_names(pitcher_file):
    df = pd.read_csv(pitcher_file)
    team_map = pd.read_csv(TEAM_MAP_FILE)[["team_code", "team_name"]]

    df = df.merge(team_map, how="left", left_on="team", right_on="team_code")
    if "team_name" in df.columns:
        df["team"] = df["team_name"]
        df = df.drop(columns=["team_code", "team_name"], errors="ignore")

    df.to_csv(pitcher_file, index=False)

def main():
    update_team_names(HOME_FILE)
    update_team_names(AWAY_FILE)

if __name__ == "__main__":
    main()
