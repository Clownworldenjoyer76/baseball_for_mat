
import pandas as pd
import os
from datetime import datetime

# Load the player-team master file
master_df = pd.read_csv("data/processed/player_team_master.csv")

# Input normalized batter and pitcher files
batter_file = "data/normalized/batters_normalized.csv"
pitcher_file = "data/normalized/pitchers_normalized.csv"

# Output folders
output_folder = "data/tagged"
output_totals_file = "data/output/player_totals.txt"

os.makedirs(output_folder, exist_ok=True)
os.makedirs("data/output", exist_ok=True)

# Load and tag each file
def tag_players(file_path, player_type):
    df = pd.read_csv(file_path)
    if "last_name, first_name" not in df.columns:
        print(f"❌ Column 'last_name, first_name' not found in {file_path}")
        return pd.DataFrame()

    merged = df.merge(
        master_df,
        how="left",
        left_on="last_name, first_name",
        right_on="name",
        suffixes=("", "_master")
    )

    output_file = os.path.join(output_folder, os.path.basename(file_path))
    merged.to_csv(output_file, index=False)
    print(f"✅ Tagged {player_type}: {output_file}")

    return merged

batters_tagged = tag_players(batter_file, "batters")
pitchers_tagged = tag_players(pitcher_file, "pitchers")

# Output totals
with open(output_totals_file, "w") as f:
    f.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
")
    f.write(f"Tagged Batters: {len(batters_tagged)}
")
    f.write(f"Tagged Pitchers: {len(pitchers_tagged)}
")
