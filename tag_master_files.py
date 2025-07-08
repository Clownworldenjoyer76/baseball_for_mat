import pandas as pd
import os

# Input files
batters_file = "data/master/batters.csv"
pitchers_file = "data/master/pitchers.csv"
master_map_file = "data/processed/player_team_master.csv"

# Output folder
output_folder = "data/tagged"
os.makedirs(output_folder, exist_ok=True)

# Load player -> team map
master_df = pd.read_csv(master_map_file)

# -- TAG BATTERS --
if os.path.exists(batters_file):
    batters_df = pd.read_csv(batters_file)
    print("📋 Batters CSV columns:", batters_df.columns.tolist())

    # Detect correct batter name column
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
        tagged_batters = batters_df.merge(
            master_df[master_df["type"] == "batter"],
            on=["name", "type"],
            how="left"
        )
        tagged_batters.to_csv(os.path.join(output_folder, "batters_tagged.csv"), index=False)
        print(f"✅ batters_tagged.csv created with {len(tagged_batters)} rows")
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
        tagged_pitchers = pitchers_df.merge(
            master_df[master_df["type"] == "pitcher"],
            on=["name", "type"],
            how="left"
        )
        tagged_pitchers.to_csv(os.path.join(output_folder, "pitchers_tagged.csv"), index=False)
        print(f"✅ pitchers_tagged.csv created with {len(tagged_pitchers)} rows")
    else:
        print("❌ Missing 'last_name, first_name' column in pitchers.csv")
else:
    print(f"⚠️ Missing file: {pitchers_file}")
##
# Find unmatched pitchers
unmatched = pitchers_df[~pitchers_df["name"].isin(tagged_pitchers["name"])]
unmatched.to_csv("data/tagged/unmatched_pitchers.csv", index=False)
print(f"⚠️ Unmatched pitchers: {len(unmatched)} written to unmatched_pitchers.csv")
