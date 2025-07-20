import pandas as pd

# Input file paths
matchup_stats_path = "data/final/matchup_stats.csv"
team_name_master_path = "data/Data/team_name_master.csv"
todaysgames_path = "data/raw/todaysgames_normalized.csv"

# Output file path
output_path = "data/final/matchup.csv"

# Load input files
matchup_stats = pd.read_csv(matchup_stats_path)
team_name_master = pd.read_csv(team_name_master_path)
todaysgames = pd.read_csv(todaysgames_path)

# Map team name to team code
name_map = dict(zip(team_name_master['team_name'], team_name_master['team_code']))
matchup_stats['normalized_team'] = matchup_stats['team'].map(name_map)

# Define function to get matchup string
def get_matchup(team, todaysgames_df):
    for _, row in todaysgames_df.iterrows():
        if team == row['away_team']:
            return f"{row['away_team']} vs {row['home_team']}"
        if team == row['home_team']:
            return f"{row['away_team']} vs {row['home_team']}"
    return ""

# Apply matchup
matchup_stats['matchup'] = matchup_stats['normalized_team'].apply(lambda t: get_matchup(t, todaysgames))

# Drop temp column
matchup_stats.drop(columns=['normalized_team'], inplace=True)

# Save to output
matchup_stats.to_csv(output_path, index=False)
