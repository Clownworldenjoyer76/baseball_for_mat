import pandas as pd
import os

# File paths
batters_path = "data/tagged/batters_tagged.csv"
pitchers_path = "data/tagged/pitchers_tagged.csv"
clean_folder = "data/cleaned"
os.makedirs(clean_folder, exist_ok=True)

# Clean batters
if os.path.exists(batters_path):
    batters_df = pd.read_csv(batters_path)
    before = len(batters_df)
    batters_df = batters_df.drop_duplicates(subset=["last_name, first_name", "team", "type"])
    after = len(batters_df)
    batters_df.to_csv(os.path.join(clean_folder, "batters_tagged_cleaned.csv"), index=False)
    print(f"🧼 Batters deduplicated: {before} → {after}")

# Clean pitchers
if os.path.exists(pitchers_path):
    pitchers_df = pd.read_csv(pitchers_path)
    before = len(pitchers_df)
    pitchers_df = pitchers_df.drop_duplicates(subset=["last_name, first_name", "team", "type"])
    after = len(pitchers_df)
    pitchers_df.to_csv(os.path.join(clean_folder, "pitchers_tagged_cleaned.csv"), index=False)
    print(f"🧼 Pitchers deduplicated: {before} → {after}")
