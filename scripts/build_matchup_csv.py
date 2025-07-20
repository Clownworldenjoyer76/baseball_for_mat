import pandas as pd
import subprocess

# File paths
MATCHUP_STATS_FILE = "data/final/matchup_stats.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
OUTPUT_FILE = "data/final/matchup.csv"

def main():
    # Load data
    df = pd.read_csv(MATCHUP_STATS_FILE)
    team_map = pd.read_csv(TEAM_MAP_FILE)
    games = pd.read_csv(GAMES_FILE)

    # Verify required columns
    if "team_name" not in team_map.columns or "map_to" not in team_map.columns:
        raise ValueError("team_name_master.csv must include 'team_name' and 'map_to' columns")

    # Normalize names using team_name map
    name_map = dict(zip(team_map["team_name"], team_map["map_to"]))
    df["team_normalized"] = df["team"].map(name_map)
    df["name_normalized"] = df["name"].map(name_map)

    # Merge matchup based on normalized team and game list
    def get_matchup(row):
        for _, game in games.iterrows():
            if row["team_normalized"] == game["home_team"]:
                return f"{game['away_team']} vs {game['home_team']}"
            if row["team_normalized"] == game["away_team"]:
                return f"{game['away_team']} vs {game['home_team']}"
        return "UNKNOWN"

    df["matchup"] = df.apply(get_matchup, axis=1)

    # Save output
    df.to_csv(OUTPUT_FILE, index=False)

    # Git identity and push
    subprocess.run(["git", "config", "--global", "user.email", "runner@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
    subprocess.run(["git", "add", OUTPUT_FILE])
    subprocess.run(["git", "commit", "-m", "Build matchup.csv from normalized data"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
