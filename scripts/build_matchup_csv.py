import pandas as pd

# Input paths
MATCHUP_STATS_PATH = "data/final/matchup_stats.csv"
TEAM_NAME_MASTER_PATH = "data/Data/team_name_master.csv"
TODAYSGAMES_PATH = "data/raw/todaysgames_normalized.csv"
OUTPUT_PATH = "data/final/matchup.csv"

# Load data
matchup_stats = pd.read_csv(MATCHUP_STATS_PATH)
team_master = pd.read_csv(TEAM_NAME_MASTER_PATH)
todays_games = pd.read_csv(TODAYSGAMES_PATH)

# Normalize for matching
matchup_stats["name_normalized"] = matchup_stats["name"].str.lower().str.strip()
team_master["team_name_normalized"] = team_master["team_name"].str.lower().str.strip()

# Merge to attach team_code by matching name → team_name
merged = pd.merge(
    matchup_stats,
    team_master[["team_name_normalized", "team_code"]],
    left_on="name_normalized",
    right_on="team_name_normalized",
    how="left"
)

# Create matchup column
def resolve_matchup(team_code, games_df):
    for _, row in games_df.iterrows():
        if team_code == row["home_team"]:
            return f"{row['away_team']} vs {row['home_team']}"
        elif team_code == row["away_team"]:
            return f"{row['away_team']} vs {row['home_team']}"
    return ""

merged["matchup"] = merged["team_code"].apply(lambda t: resolve_matchup(t, todays_games))

# Drop temp columns
merged.drop(columns=["name_normalized", "team_name_normalized"], inplace=True)

# Write to final output
merged.to_csv(OUTPUT_PATH, index=False)
