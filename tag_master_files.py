import pandas as pd
import os

# Input files
batters_file = "data/master/batters.csv"
pitchers_file = "data/master/pitchers.csv"
master_map_file = "data/processed/player_team_master.csv"

# Output folders
tagged_folder = "data/tagged"
output_folder = "data/output"
os.makedirs(tagged_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)

# Load player -> team map
master_df = pd.read_csv(master_map_file)

# Initialize counters
batter_count = 0
pitcher_count = 0

# -- TAG BATTERS --
if os.path.exists(batters_file):
    batters_df = pd.read_csv(batters_file)
    print("📋 Batters CSV columns:", batters_df.columns.tolist())

    batter_name_col = None
    for col in batters_df.columns:
        normalized = col.strip().lower()
        if normalized in [
            "last name, first name", 
            "last name,first name", 
            "last_name, first_name", 
            "last_name,first_name",
            "name"
        ]:
            batter_name_col = col
            break

    if batter_name_col:
        batters_df = batters_df.rename(columns={batter_name_col: "name"})
        batters_df["type"] = "batter"
        batter_count = len(batters_df)
        tagged_batters = batters_df.merge(
            master_df[master_df["type"] == "batter"],
            on=["name", "type"],
            how="left"
        )
        tagged_batters.to_csv(os.path.join(tagged_folder, "batters_tagged.csv"), index=False)
        print(f"✅ batters_tagged.csv created with {len(tagged_batters)} rows")

        unmatched = tagged_batters[tagged_batters["team"].isna()]
        unmatched.to_csv(os.path.join(tagged_folder, "unmatched_batters.csv"), index=False)
        print(f"⚠️ Unmatched batters (missing team): {len(unmatched)} written to unmatched_batters.csv")
    else:
        print("❌ Could not find valid 'name' column in batters.csv")
else:
    print(f"⚠️ Missing file: {batters_file}")

# -- TAG PITCHERS --
if os.path.exists(pitchers_file):
    pitchers_df = pd.read_csv(pitchers_file)
    if "last_name, first_name" in pitchers_df.columns:
        pitchers_df = pitchers_df.rename(columns={"last_name, first_name": "name"})
        pitchers_df["type"] = "pitcher"
        pitcher_count = len(pitchers_df)
        tagged_pitchers = pitchers_df.merge(
            master_df[master_df["type"] == "pitcher"],
            on=["name", "type"],
            how="left"
        )
        tagged_pitchers.to_csv(os.path.join(tagged_folder, "pitchers_tagged.csv"), index=False)
        print(f"✅ pitchers_tagged.csv created with {len(tagged_pitchers)} rows")

        unmatched = tagged_pitchers[tagged_pitchers["team"].isna()]
        unmatched.to_csv(os.path.join(tagged_folder, "unmatched_pitchers.csv"), index=False)
        print(f"⚠️ Unmatched pitchers (missing team): {len(unmatched)} written to unmatched_pitchers.csv")
    else:
        print("❌ Missing 'last_name, first_name' column in pitchers.csv")
else:
    print(f"⚠️ Missing file: {pitchers_file}")

# Write totals summary
with open(os.path.join(output_folder, "player_totals.txt"), "w") as f:
    f.write(f"Total batters in CSV: {batter_count}\n")
    f.write(f"Total pitchers in CSV: {pitcher_count}\n")

print("📄 Totals written to player_totals.txt")
