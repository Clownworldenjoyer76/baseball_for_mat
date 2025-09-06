import pandas as pd
from pathlib import Path
from scipy.stats import zscore, norm

# Input files
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
PROPS_FILE = Path("data/_projections/pitcher_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_props_z_expanded.csv")

# Load data
df_xtra = pd.read_csv(XTRA_FILE)
df_props = pd.read_csv(PROPS_FILE)

# Ensure player_id is string in both
df_xtra["player_id"] = df_xtra["player_id"].astype(str)
df_props["player_id"] = df_props["player_id"].astype(str)

# Merge to get projection stats per player
id_vars = ["player_id", "name", "team"]
stat_vars = ["k", "bb"]
merged = pd.merge(
    df_props[id_vars],
    df_xtra[["player_id"] + stat_vars],
    on="player_id",
    how="inner"
)

# Normalize column names
merged.columns = merged.columns.str.strip().str.lower()

# Reshape to long format (1 row per player per prop type)
expanded = pd.melt(
    merged,
    id_vars=["player_id", "name", "team"],
    value_vars=["k", "bb"],
    var_name="prop_type",
    value_name="projection"
)

# Rename prop_type from internal names
expanded["prop_type"] = expanded["prop_type"].map({
    "k": "strikeouts",
    "bb": "walks"
})

# Define betting lines
lines = {
    "strikeouts": [5.5, 6.5, 7.5],
    "walks": [1.5, 2.5, 3.5]
}
lines_df = pd.DataFrame(lines.items(), columns=["prop_type", "line"]).explode("line")

# Combine player projections with line values
final_expanded = pd.merge(expanded, lines_df, on="prop_type")

# Compute z_score safely (fallback to 0 if constant)
final_expanded["z_score"] = final_expanded.groupby(["prop_type", "line"])["projection"].transform(
    lambda x: zscore(x, ddof=0) if x.nunique() > 1 else pd.Series([0] * len(x), index=x.index)
)

# Invert z_score for walks (lower is better)
final_expanded.loc[final_expanded["prop_type"] == "walks", "z_score"] *= -1

# Calculate over probability
final_expanded["over_probability"] = 1 - norm.cdf(final_expanded["z_score"])

# Round outputs
final_expanded["projection"] = final_expanded["projection"].round(3)
final_expanded["z_score"] = final_expanded["z_score"].round(4)
final_expanded["over_probability"] = final_expanded["over_probability"].round(4)

# Final output
final_cols = ["player_id", "name", "team", "prop_type", "line", "projection", "z_score", "over_probability"]
final = final_expanded[final_cols].sort_values(by=["name", "prop_type", "line"]).reset_index(drop=True)

# Save
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Wrote pitcher props to: {OUTPUT_FILE}")


import os
print("✅ File exists:", os.path.exists("data/_projections/pitcher_props_z_expanded.csv"))
print("✅ Preview:\n", final.head())
