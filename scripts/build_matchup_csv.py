import pandas as pd
import subprocess

# File paths
MATCHUP_STATS_PATH = "data/final/matchup_stats.csv"
TEAM_MAP_PATH = "data/Data/team_name_master.csv"
GAMES_PATH = "data/raw/todaysgames_normalized.csv"
OUTPUT_PATH = "data/final/matchup.csv"

def main():
    # Load data
    matchup_df = pd.read_csv(MATCHUP_STATS_PATH)
    team_map_df = pd.read_csv(TEAM_MAP_PATH)
    games_df = pd.read_csv(GAMES_PATH)

    # Confirm required columns exist
    for col in ["team", "name", "type"]:
        if col not in matchup_df.columns:
            raise ValueError(f"Missing required column '{col}' in matchup_stats.csv")
    for col in ["team_name", "clean_team_name"]:
        if col not in team_map_df.columns:
            raise ValueError(f"Missing required column '{col}' in team_name_master.csv")
    for col in ["home_team", "away_team"]:
        if col not in games_df.columns:
            raise ValueError(f"Missing required column '{col}' in todaysgames_normalized.csv")

    # Normalize 'team' column using team_name_master.csv
    team_map = dict(zip(team_map_df["team_name"], team_map_df["clean_team_name"]))
    matchup_df["team"] = matchup_df["team"].map(team_map).fillna(matchup_df["team"])

    # Create 'matchup' column: "away_team vs home_team"
    def get_matchup(team):
        match = games_df[(games_df["home_team"] == team) | (games_df["away_team"] == team)]
        if match.empty:
            return ""
        return f"{match.iloc[0]['away_team']} vs {match.iloc[0]['home_team']}"

    matchup_df["matchup"] = matchup_df["team"].apply(get_matchup)

    # Output to CSV
    matchup_df.to_csv(OUTPUT_PATH, index=False)

    # Git commit and push
    subprocess.run(["git", "config", "--global", "user.email", "runner@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
    subprocess.run(["git", "add", OUTPUT_PATH])
    subprocess.run(["git", "commit", "-m", "Add final matchup.csv"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
