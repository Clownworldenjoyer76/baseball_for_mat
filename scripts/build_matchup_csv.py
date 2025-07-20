
import pandas as pd
import subprocess

# Input files
MATCHUP_STATS_FILE = "data/final/matchup_stats.csv"
TEAM_NAME_MAP_FILE = "data/Data/team_name_map.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"

# Output
OUTPUT_FILE = "data/final/matchup.csv"

def main():
    matchup_df = pd.read_csv(MATCHUP_STATS_FILE)
    team_map = pd.read_csv(TEAM_NAME_MAP_FILE)
    games_df = pd.read_csv(GAMES_FILE)

    # Normalize team names
    if "team" not in matchup_df.columns:
        raise ValueError("Missing 'team' column in matchup_stats.csv")
    if "standard_name" not in team_map.columns or "name" not in team_map.columns:
        raise ValueError("team_name_map.csv must include 'name' and 'standard_name' columns")

    matchup_df = matchup_df.merge(team_map, left_on="team", right_on="name", how="left")
    matchup_df["team"] = matchup_df["standard_name"].fillna(matchup_df["team"])
    matchup_df.drop(columns=["name_y", "standard_name"], errors="ignore", inplace=True)

    # Normalize 'name' column
    if "name" in matchup_df.columns:
        matchup_df["name"] = matchup_df["name"].str.strip().str.title()

    # Add matchup column
    if "home_team" not in games_df.columns or "away_team" not in games_df.columns:
        raise ValueError("Missing home_team or away_team in todaysgames_normalized.csv")

    def determine_matchup(row):
        for _, game in games_df.iterrows():
            if row["team"] == game["home_team"] or row["team"] == game["away_team"]:
                return f"{game['away_team']} vs {game['home_team']}"
        return "Unknown"

    matchup_df["matchup"] = matchup_df.apply(determine_matchup, axis=1)

    # Output final file
    matchup_df.to_csv(OUTPUT_FILE, index=False)

    subprocess.run(["git", "add", OUTPUT_FILE])
    subprocess.run(["git", "commit", "-m", "Add normalized matchup CSV"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
