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

# --- CORRECTED SCRIPT LOGIC ---

# Standardize player_id as string in both files
df_xtra["player_id"] = df_xtra["player_id"].astype(str)
df_props["player_id"] = df_props["player_id"].astype(str)

# Define which columns to take from each file
props_id_vars = ["player_id", "name", "team"]
xtra_stat_vars = ["player_id", "strikeouts", "walks"]

# Select the correct columns from each dataframe BEFORE merging
df_props_subset = df_props[props_id_vars]
df_xtra_subset = df_xtra[xtra_stat_vars]

# Merge the two subsets to get the stats joined to the projected players
merged = pd.merge(df_props_subset, df_xtra_subset, on="player_id", how="inner")


# Melt to 1 row per prop
expanded = pd.melt(
    merged,
    id_vars=["player_id", "name", "team"],
    value_vars=["strikeouts", "walks"], # Use correct column names
    var_name="prop_type",
    value_name="projection"
)

# Map to proper prop_type names (now the keys match the columns)
expanded["prop_type"] = expanded["prop_type"].map({
    "strikeouts": "strikeouts",
    "walks": "walks"
})

# Define the lines for each prop
lines = {
    "strikeouts": [5.5, 6.5, 7.5],
    "walks": [1.5, 2.5, 3.5]
}

# Create a list of DataFrames, one for each prop and line
dfs_to_concat = []
for prop, line_values in lines.items():
    prop_df = expanded[expanded["prop_type"] == prop].copy()
    for line in line_values:
        line_df = prop_df.copy()
        line_df["line"] = line
        dfs_to_concat.append(line_df)

# Combine all the new prop-line dataframes
final_expanded = pd.concat(dfs_to_concat, ignore_index=True)

# Compute z-score (per prop_type and line)
final_expanded["ultimate_z"] = final_expanded.groupby(["prop_type", "line"])["projection"].transform(zscore)

# Invert z-score for walks (so a lower walk projection gets a better z-score)
final_expanded.loc[final_expanded["prop_type"] == "walks", "ultimate_z"] *= -1

# Round for frontend use
final_expanded["ultimate_z"] = final_expanded["ultimate_z"].round(4)
final_expanded["projection"] = final_expanded["projection"].round(3)

# Final column order and sort
final = final_expanded[["player_id", "name", "team", "prop_type", "line", "projection", "ultimate_z"]]
final = final.sort_values(by=["name", "prop_type", "line"]).reset_index(drop=True)

# Save
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Wrote pitcher props to: {OUTPUT_FILE}")
