import pandas as pd
from pathlib import Path
from scipy.stats import zscore

# Load source
INPUT_FILE = Path("data/_projections/batter_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_z_expanded.csv")

# Read base batter file (1 row per player)
df = pd.read_csv(INPUT_FILE)

# Clean column names
df.columns = df.columns.str.strip().str.lower()

# --- NEW: Filter for only batters ---
df = df[df["type"] == "batter"].copy()

# Ensure required fields exist (now including walk and strikeout)
required_fields = ["player_id", "name", "team", "total_bases_projection", "total_hits_projection", "avg_hr", "walk", "strikeout"]
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

# Map to proper prop_type
expanded["prop_type"] = expanded["prop_type"].map({
    "total_bases_projection": "total_bases",
    "total_hits_projection": "hits",
    "avg_hr": "home_runs",
    "walk": "walks",
    "strikeout": "strikeouts"
})

# --- UPDATED: Handle dual hits line and add new props ---

# Create a copy for the second hits line
hits_1_5 = expanded[expanded["prop_type"] == "hits"].copy()
hits_1_5["line"] = 1.5

# Assign the default lines
expanded["line"] = expanded["prop_type"].map({
    "total_bases": 1.5,
    "hits": 0.5,
    "home_runs": 0.5,
    "walks": 0.5,
    "strikeouts": 0.5
})

# Combine the original data with the new 1.5 hits line data
final_expanded = pd.concat([expanded, hits_1_5], ignore_index=True)


# Compute z-score (per prop_type and now per line for hits)
final_expanded["ultimate_z"] = final_expanded.groupby(["prop_type", "line"])["projection"].transform(zscore)


# Round for frontend use
final_expanded["ultimate_z"] = final_expanded["ultimate_z"].round(4)
final_expanded["projection"] = final_expanded["projection"].round(3)

# Final column order and sort
final = final_expanded[["player_id", "name", "team", "prop_type", "line", "projection", "ultimate_z"]]
final = final.sort_values(by=["name", "prop_type", "line"]).reset_index(drop=True)


final.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}")

