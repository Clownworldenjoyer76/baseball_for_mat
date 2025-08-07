import pandas as pd
from pathlib import Path
from scipy.stats import zscore

# Input files
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
PROPS_FILE = Path("data/_projections/pitcher_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_props_z_expanded.csv")

# Load data
df_xtra = pd.read_csv(XTRA_FILE)
df_props = pd.read_csv(PROPS_FILE)

# Standardize player_id as string to ensure successful merges
df_xtra["player_id"] = df_xtra["player_id"].astype(str)
df_props["player_id"] = df_props["player_id"].astype(str)

# Define columns for merging
# Player info comes from PROPS_FILE
id_vars = ["player_id", "name", "team"]
# Stats (k, bb) come from XTRA_FILE
stat_vars = ["k", "bb"] 

# Correctly merge the two DataFrames
# This takes player info from df_props and stats from df_xtra
merged = pd.merge(
    df_props[id_vars],
    df_xtra[["player_id"] + stat_vars],
    on="player_id",
    how="inner"
)

# Clean column names
merged.columns = merged.columns.str.strip().str.lower()

# Melt to 1 row per prop for easier processing
expanded = pd.melt(
    merged,
    id_vars=["player_id", "name", "team"],
    value_vars=["k", "bb"],
    var_name="prop_type",
    value_name="projection"
)

# Map internal stat names to user-friendly prop names
expanded["prop_type"] = expanded["prop_type"].map({
    "k": "strikeouts",
    "bb": "walks"
})

# Define the betting lines for each prop type
lines = {
    "strikeouts": [5.5, 6.5, 7.5],
    "walks": [1.5, 2.5, 3.5]
}

# Efficiently expand the data to include all lines for each player/prop
# This creates a DataFrame of lines and merges it with the player data
lines_df = pd.DataFrame(lines.items(), columns=['prop_type', 'line']).explode('line')
final_expanded = pd.merge(expanded, lines_df, on='prop_type')

# Compute z-score, grouped by prop_type and line
# This compares each player's projection to the mean for that specific line
final_expanded["ultimate_z"] = final_expanded.groupby(["prop_type", "line"])["projection"].transform(zscore)

# Invert z-score for walks, so a higher score is always better
# (since lower walk projections are favorable)
final_expanded.loc[final_expanded["prop_type"] == "walks", "ultimate_z"] *= -1

# Round values for cleaner output
final_expanded["ultimate_z"] = final_expanded["ultimate_z"].round(4)
final_expanded["projection"] = final_expanded["projection"].round(3)

# Select and order final columns for the output file
final_cols = ["player_id", "name", "team", "prop_type", "line", "projection", "ultimate_z"]
final = final_expanded[final_cols]
final = final.sort_values(by=["name", "prop_type", "line"]).reset_index(drop=True)

# Save the final DataFrame to a CSV file
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Wrote pitcher props to: {OUTPUT_FILE}")

