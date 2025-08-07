import pandas as pd
import numpy as np
from pathlib import Path

# Input files
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
PROPS_FILE = Path("data/_projections/pitcher_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_mega_zscores.csv")

# Load data
df_xtra = pd.read_csv(XTRA_FILE)
df_props = pd.read_csv(PROPS_FILE)

# Standardize player_id as string
df_xtra["player_id"] = df_xtra["player_id"].astype(str)
df_props["player_id"] = df_props["player_id"].astype(str)

# Merge on player_id
merged = pd.merge(df_props, df_xtra, on="player_id", how="inner")

# Filter to today's starters (only those present in pitcher_props_projected.csv)
# Assumes that only today's starters are in the pitcher_props_projected.csv file
# You can modify this if you later include non-starters in that file

# Select relevant stat columns
stat_cols = ["k", "ip", "era", "whip", "bb", "er"]

# Ensure numeric and handle any missing data
for col in stat_cols:
    merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)

# Normalize (z-score)
merged["z_score"] = merged[stat_cols].apply(
    lambda row: np.mean([(row[c] - merged[c].mean()) / merged[c].std(ddof=0) for c in stat_cols]),
    axis=1
)

# Round z_score
merged["z_score"] = merged["z_score"].round(6)

# Select final columns
out = merged[["player_id", "name_x", "team", "z_score"]].rename(columns={"name_x": "name"})

# Sort
out = out.sort_values(by="z_score", ascending=False)

# Save
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(OUTPUT_FILE, index=False)
