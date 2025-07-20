import pandas as pd
import subprocess

# File paths
MATCHUP_STATS_PATH = "data/final/matchup_stats.csv"
TEAM_MAP_PATH = "data/Data/team_name_master.csv"
GAMES_PATH = "data/raw/todaysgames_normalized.csv"
OUTPUT_PATH = "data/final/matchup.csv"

def main():
    # Load files
    matchup_df = pd.read_csv(MATCHUP_STATS_PATH)
    team_map_df = pd.read_csv(TEAM_MAP_PATH)
    games_df = pd.read_csv(GAMES_PATH)

    # Validate all required columns exist
    if 'team_name' not in team_map_df.columns or 'mapped_team' not in team_map_df.columns:
        raise ValueError("team_name_master.csv must include 'team_name' and 'mapped_team' columns")
    if 'team' not in matchup_df.columns or 'name' not in matchup_df.columns or 'type' not in matchup_df.columns:
        raise ValueError("matchup_stats.csv missing required columns: team, name, or type")
    if 'home_team' not in games_df.columns or 'away_team' not in games_df.columns:
        raise ValueError("todaysgames_normalized.csv missing required columns: home_team or away_team")

    # Normalize team names
    name_map = dict(zip(team_map_df["team_name"], team_map_df["mapped_team"]))
    matchup_df["team"] = matchup_df["team"].map(name_map).fillna(matchup_df["team"])

    # Assign matchup field
    def assign_matchup(row):
        team = row["team"]
        match = games_df[(games_df["home_team"] == team) | (games_df["away_team"] == team)]
        if match.empty:
            return ""
        return f"{match.iloc[0]['away_team']} vs {match.iloc[0]['home_team']}"

    matchup_df["matchup"] = matchup_df.apply(assign_matchup, axis=1)

    # Save output
    matchup_df.to_csv(OUTPUT_PATH, index=False)

    # Commit and push
    subprocess.run(["git", "config", "--global", "user.email", "runner@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
    subprocess.run(["git", "add", OUTPUT_PATH])
    subprocess.run(["git", "commit", "-m", "Add final matchup.csv"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
