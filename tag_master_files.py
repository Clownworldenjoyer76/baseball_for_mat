import os
import pandas as pd
from datetime import datetime

# Load normalized data
batters_df = pd.read_csv("data/normalized/batters_normalized.csv")
pitchers_df = pd.read_csv("data/normalized/pitchers_normalized.csv")
player_team_master = pd.read_csv("data/processed/player_team_master.csv")

# Process and tag logic placeholder
# [Insert your actual processing logic here]

# Write out log with timestamp
with open("data/processed/tagging_log.txt", "w") as f:
    f.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("Tagging complete.")
