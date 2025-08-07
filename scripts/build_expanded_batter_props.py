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

# Ensure required fields exist
required_fields = ["player_id", "name", "team", "total_bases_projection", "total_hits_projection", "avg_hr"]
for field in required_fields:
    if field not in df.columns:
        raise ValueError(f"Missing required column: {field}")

# Melt to 1 row per prop
expanded = pd.melt(
    df,
    id_vars=["player_id", "name", "team"],
    value_vars=["total_bases_projection", "total_hits_projection", "avg_hr"],
    var_name="prop_type",
    value_name="projection"
)

# Map to proper prop_type and assumed line
expanded["prop_type"] = expanded["prop_type"].map({
    "total_bases_projection": "total_bases",
    "total_hits_projection": "hits",
    "avg_hr": "home_runs"
})
expanded["line"] = expanded["prop_type"].map({
    "total_bases": 1.5,
    "hits": 0.5,
    "home_runs": 0.5
})

# Compute z-score (per prop_type)
expanded["ultimate_z"] = expanded.groupby("prop_type")["projection"].transform(zscore)

# Round for frontend use
expanded["ultimate_z"] = expanded["ultimate_z"].round(4)
expanded["projection"] = expanded["projection"].round(3)

# Final column order
final = expanded[["player_id", "name", "team", "prop_type", "line", "projection", "ultimate_z"]]
final.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}")
