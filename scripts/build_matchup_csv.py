import pandas as pd
import subprocess

# Input files
MATCHUP_STATS = "data/final/matchup_stats.csv"
TEAM_NAME_MAP = "data/Data/team_name_master.csv"
TODAYS_GAMES = "data/raw/todaysgames_normalized.csv"
OUTPUT_FILE = "data/final/matchup.csv"

def main():
    matchup_df = pd.read_csv(MATCHUP_STATS)
    team_map_df = pd.read_csv(TEAM_NAME_MAP)
    games_df = pd.read_csv(TODAYS_GAMES)

    # Verify required columns
    if "team_name" not in team_map_df.columns or "clean_team_name" not in team_map_df.columns:
        raise ValueError("team_name_master.csv must include 'team_name' and 'clean_team_name' columns")

    if "name" not in matchup_df.columns or "team" not in matchup_df.columns or "type" not in matchup_df.columns:
        raise ValueError("matchup_stats.csv must include 'name', 'team', and 'type' columns")

    if "home_team" not in games_df.columns or "away_team" not in games_df.columns:
        raise ValueError("todaysgames_normalized.csv must include 'home_team' and 'away_team' columns")

    # Normalize team names in matchup_df using team_name_master.csv
    team_name_dict = dict(zip(team_map_df["team_name"], team_map_df["clean_team_name"]))
    matchup_df["team"] = matchup_df["team"].map(team_name_dict).fillna(matchup_df["team"])

    # Create matchup column by mapping teams from todaysgames_normalized
    def assign_matchup(row):
        for _, game in games_df.iterrows():
            if row["team"] == game["home_team"] or row["team"] == game["away_team"]:
                return f"{game['away_team']} vs {game['home_team']}"
        return "Unknown"

    matchup_df["matchup"] = matchup_df.apply(assign_matchup, axis=1)

    # Output
    matchup_df.to_csv(OUTPUT_FILE, index=False)

    # Git setup
    subprocess.run(["git", "config", "--global", "user.email", "runner@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
    subprocess.run(["git", "add", OUTPUT_FILE])
    subprocess.run(["git", "commit", "-m", "Add normalized matchup output"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
