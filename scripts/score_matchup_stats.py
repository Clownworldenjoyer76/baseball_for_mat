import pandas as pd
import os

# Required file paths
MATCHUP_STATS_PATH = "data/final/matchup_stats.csv"
TEAM_NAME_MASTER_PATH = "data/Data/team_name_master.csv"
TODAYSGAMES_PATH = "data/raw/todaysgames_normalized.csv"
OUTPUT_PATH = "data/final/matchup.csv"
DEBUG_PATH = "data/final/matchup_debug.txt"

# Load input files
matchup_stats = pd.read_csv(MATCHUP_STATS_PATH)
team_master = pd.read_csv(TEAM_NAME_MASTER_PATH)
todays_games = pd.read_csv(TODAYSGAMES_PATH)

# Normalize values
matchup_stats["team_normalized"] = matchup_stats["team"].str.lower().str.strip()
team_master["team_name_normalized"] = team_master["team_name"].str.lower().str.strip()

# Merge on normalized team
merged = pd.merge(
    matchup_stats,
    team_master[["team_name_normalized", "team_code"]],
    left_on="team_normalized",
    right_on="team_name_normalized",
    how="left"
)

# Generate 'matchup' column
def resolve_matchup(team_code, games_df):
    for _, row in games_df.iterrows():
        if team_code == row["home_team"]:
            return f"{row['away_team']} vs {row['home_team']}"
        elif team_code == row["away_team"]:
            return f"{row['away_team']} vs {row['home_team']}"
    return "unmatched"

merged["matchup"] = merged["team_code"].apply(lambda t: resolve_matchup(t, todays_games))

# Preserve 'type' from matchup_stats
merged["type"] = matchup_stats["type"]

# Ensure output directory exists
os.makedirs("data/final", exist_ok=True)

# Write guaranteed CSV
merged.to_csv(OUTPUT_PATH, index=False)

# Write guaranteed debug file
with open(DEBUG_PATH, "w") as f:
    f.write(f"Total rows: {len(merged)}\n")
    f.write(f"Matched team_code: {merged['team_code'].notna().sum()}\n")
    unmatched = merged[merged['team_code'].isna()]["team"].dropna().unique().tolist()
    f.write(f"Unmatched teams (max 10): {unmatched[:10]}\n")

# Failsafe: print to console so user sees proof
print("[MATCHUP SCRIPT COMPLETE]")
print(f"CSV saved to: {OUTPUT_PATH}")
print(f"Debug saved to: {DEBUG_PATH}")
print(f"Total rows: {len(merged)}")
print(f"Matched: {merged['team_code'].notna().sum()}")
print(f"Unmatched teams (sample): {unmatched[:10]}")
