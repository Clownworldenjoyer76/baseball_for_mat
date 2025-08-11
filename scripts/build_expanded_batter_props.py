import pandas as pd
from pathlib import Path
from scipy.stats import zscore, norm

# Load source
INPUT_FILE = Path("data/_projections/batter_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_z_expanded.csv")

# Read base batter file (1 row per player)
df = pd.read_csv(INPUT_FILE)
df.columns = df.columns.str.strip().str.lower()

# Map CSV column names to what the script expects
rename_map = {
    "b_total_bases": "total_bases_projection",
    "proj_hits": "total_hits_projection",
    "proj_hr": "avg_hr",
}
df = df.rename(columns=rename_map)

# Filter for only batters
df = df[df["type"] == "batter"].copy()

# Ensure required fields exist
required_fields = [
    "player_id", "name", "team",
    "total_bases_projection", "total_hits_projection", "avg_hr",
    "walk", "strikeout"
]
for field in required_fields:
    if field not in df.columns:
        raise ValueError(f"Missing required column: {field}")

# Melt to 1 row per prop
expanded = pd.melt(
    df,
    id_vars=["player_id", "name", "team"],
    value_vars=["total_bases_projection", "total_hits_projection", "avg_hr", "walk", "strikeout"],
    var_name="prop_type",
    value_name="projection"
)

# Map to standardized prop types
expanded["prop_type"] = expanded["prop_type"].map({
    "total_bases_projection": "total_bases",
    "total_hits_projection": "hits",
    "avg_hr": "home_runs",
    "walk": "walks",
    "strikeout": "strikeouts"
})

# Duplicate hits with line = 1.5
hits_1_5 = expanded[expanded["prop_type"] == "hits"].copy()
hits_1_5["line"] = 1.5

# Default lines
expanded["line"] = expanded["prop_type"].map({
    "total_bases": 1.5,
    "hits": 0.5,
    "home_runs": 0.5,
    "walks": 0.5,
    "strikeouts": 0.5
})

# Combine hits 1.5 with original
final_expanded = pd.concat([expanded, hits_1_5], ignore_index=True)

# Z-score per prop_type + line
final_expanded["ultimate_z"] = final_expanded.groupby(["prop_type", "line"])["projection"].transform(zscore)

# Standard deviations (tuned per prop type)
std_devs = {
    "total_bases": 0.5,
    "hits": 0.3,
    "home_runs": 0.08,
    "walks": 0.4,
    "strikeouts": 0.4
}

final_expanded.loc[final_expanded["prop_type"] == "strikeouts", "projection"] /= 150

# Compute over_probability correctly
def compute_prob(row):
    sigma = std_devs.get(row["prop_type"], 0.5)
    z = (row["projection"] - row["line"]) / sigma
    return round(norm.sf(z), 4)  # Corrected: P(X > line)

final_expanded["over_probability"] = final_expanded.apply(compute_prob, axis=1)

# Round values
final_expanded["ultimate_z"] = final_expanded["ultimate_z"].round(4)
final_expanded["projection"] = final_expanded["projection"].round(3)

# Final output
final = final_expanded[[
    "player_id", "name", "team",
    "prop_type", "line", "projection",
    "ultimate_z", "over_probability"
]].sort_values(by=["name", "prop_type", "line"]).reset_index(drop=True)

# Save
final.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}")
