import pandas as pd

# Input paths
MATCHUP_STATS_PATH = "data/final/matchup_stats.csv"
TEAM_NAME_MASTER_PATH = "data/Data/team_name_master.csv"
TODAYSGAMES_PATH = "data/raw/todaysgames_normalized.csv"
OUTPUT_PATH = "data/final/matchup.csv"
DEBUG_LOG_PATH = "data/final/matchup_debug.txt"

# Load data
matchup_stats = pd.read_csv(MATCHUP_STATS_PATH)
team_master = pd.read_csv(TEAM_NAME_MASTER_PATH)
todays_games = pd.read_csv(TODAYSGAMES_PATH)

# Normalize fields
matchup_stats["team_normalized"] = matchup_stats["team"].str.lower().str.strip()
team_master["team_name_normalized"] = team_master["team_name"].str.lower().str.strip()

# Merge to attach team_code
merged = pd.merge(
    matchup_stats,
    team_master[["team_name_normalized", "team_code"]],
    left_on="team_normalized",
    right_on="team_name_normalized",
    how="left"
)

# Build matchup string
def resolve_matchup(team_code, games_df):
    for _, row in games_df.iterrows():
        if team_code == row["home_team"]:
            return f"{row['away_team']} vs {row['home_team']}"
        elif team_code == row["away_team"]:
            return f"{row['away_team']} vs {row['home_team']}"
    return ""

merged["matchup"] = merged["team_code"].apply(lambda t: resolve_matchup(t, todays_games))

# Write final output
merged.drop(columns=["team_normalized", "team_name_normalized"], inplace=True)
merged.to_csv(OUTPUT_PATH, index=False)

# Write debug log
with open(DEBUG_LOG_PATH, "w") as log:
    log.write(f"Total rows: {len(merged)}\n")
    log.write(f"Matched team_code: {merged['team_code'].notna().sum()}\n")
    unmatched = merged[merged['team_code'].isna()]["team"].unique().tolist()
    log.write(f"Unmatched teams (max 10): {unmatched[:10]}\n")
