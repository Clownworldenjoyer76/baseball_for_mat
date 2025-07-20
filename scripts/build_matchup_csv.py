import pandas as pd
import subprocess

# Input paths
MATCHUP_STATS_FILE = "data/final/matchup_stats.csv"
TEAM_NAME_MAP_FILE = "data/Data/team_name_master.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
OUTPUT_FILE = "data/final/matchup.csv"

def main():
    df = pd.read_csv(MATCHUP_STATS_FILE)
    team_map = pd.read_csv(TEAM_NAME_MAP_FILE)
    games = pd.read_csv(GAMES_FILE)

    if 'team_name' not in team_map.columns or 'standard_name' not in team_map.columns:
        raise ValueError("team_name_master.csv must include 'team_name' and 'standard_name' columns")
    if 'name' not in df.columns or 'team' not in df.columns or 'type' not in df.columns:
        raise ValueError("matchup_stats.csv must include 'name', 'team', and 'type' columns")
    if 'home_team' not in games.columns or 'away_team' not in games.columns:
        raise ValueError("todaysgames_normalized.csv must include 'home_team' and 'away_team' columns")

    # Normalize 'team' using team_name_master.csv
    df = df.merge(team_map, left_on='team', right_on='team_name', how='left')
    if df['standard_name'].isnull().any():
        missing = df[df['standard_name'].isnull()]['team'].unique()
        raise ValueError(f"Missing standard_name mapping for teams: {missing}")
    df['team'] = df['standard_name']
    df.drop(columns=['team_name', 'standard_name'], inplace=True)

    # Create matchup column
    def find_matchup(row_team):
        row = games[(games['home_team'] == row_team) | (games['away_team'] == row_team)]
        if row.empty:
            return None
        home = row.iloc[0]['home_team']
        away = row.iloc[0]['away_team']
        return f"{away} vs {home}"

    df['matchup'] = df['team'].apply(find_matchup)

    df.to_csv(OUTPUT_FILE, index=False)

    # Git identity
    subprocess.run(["git", "config", "--global", "user.email", "runner@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])

    # Commit and push
    subprocess.run(["git", "add", OUTPUT_FILE])
    subprocess.run(["git", "commit", "-m", "Create normalized matchup.csv"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
