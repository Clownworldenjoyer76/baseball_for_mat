
from pathlib import Path
import pandas as pd
import unicodedata

# Define file paths
batters_file = Path("data/master/batters.csv")
pitchers_file = Path("data/master/pitchers.csv")
lookup_file = Path("data/processed/player_team_master.csv")

tagged_batters_file = Path("data/tagged/batters_tagged.csv")
tagged_pitchers_file = Path("data/tagged/pitchers_tagged.csv")
unmatched_batters_file = Path("data/output/unmatched_batters.csv")
unmatched_pitchers_file = Path("data/output/unmatched_pitchers.csv")
player_totals_file = Path("data/output/player_totals.txt")

# Create output directories if they don't exist
tagged_batters_file.parent.mkdir(parents=True, exist_ok=True)
unmatched_batters_file.parent.mkdir(parents=True, exist_ok=True)

# Load data
batters_df = pd.read_csv(batters_file)
pitchers_df = pd.read_csv(pitchers_file)
lookup_df = pd.read_csv(lookup_file)

# Rename for consistency
lookup_df = lookup_df.rename(columns={"name": "last_name, first_name"})

# Function to strip accents and normalize names
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return ''.join([c for c in normalized if not unicodedata.combining(c)])

def normalize_name(name):
    return strip_accents(name).lower().strip()

# Apply normalization
batters_df["normalized_name"] = batters_df["last_name, first_name"].apply(normalize_name)
pitchers_df["normalized_name"] = pitchers_df["last_name, first_name"].apply(normalize_name)
lookup_df["normalized_name"] = lookup_df["last_name, first_name"].apply(normalize_name)

# Use normalized column for merging
def tag_players(df, player_type):
    df["type"] = player_type
    merged = df.merge(
        lookup_df[["normalized_name", "team"]],
        on="normalized_name",
        how="left"
    )
    matched = merged[merged["team"].notna()].copy()
    unmatched = merged[merged["team"].isna()][["last_name, first_name"]].copy()
    return matched.drop(columns=["normalized_name"]), unmatched

# Tag batters and pitchers
tagged_batters, unmatched_batters = tag_players(batters_df, "batter")
tagged_pitchers, unmatched_pitchers = tag_players(pitchers_df, "pitcher")

# Save outputs
tagged_batters.to_csv(tagged_batters_file, index=False)
tagged_pitchers.to_csv(tagged_pitchers_file, index=False)
unmatched_batters.to_csv(unmatched_batters_file, index=False)
unmatched_pitchers.to_csv(unmatched_pitchers_file, index=False)

# Clear and refresh player_totals.txt
with open(player_totals_file, "w") as f:
    f.write(f"Total Batters: {len(batters_df)}\n")
    f.write(f"Matched Batters: {len(tagged_batters)}\n")
    f.write(f"Unmatched Batters: {len(unmatched_batters)}\n\n")
    f.write(f"Total Pitchers: {len(pitchers_df)}\n")
    f.write(f"Matched Pitchers: {len(tagged_pitchers)}\n")
    f.write(f"Unmatched Pitchers: {len(unmatched_pitchers)}\n")
