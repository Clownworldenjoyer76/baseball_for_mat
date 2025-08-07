import pandas as pd
from pathlib import Path
from scipy.stats import zscore

# File paths
INPUT_PROPS = Path("data/_projections/pitcher_props_projected.csv")
XTRA_STATS = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_mega_z.csv")

# Load data
df_base = pd.read_csv(INPUT_PROPS)
df_xtra = pd.read_csv(XTRA_STATS)

# Ensure consistent ID types
df_base["player_id"] = df_base["player_id"].astype(str).str.strip()
df_xtra["player_id"] = df_xtra["player_id"].astype(str).str.strip()

# Merge on player_id
df = df_base.merge(
    df_xtra[["player_id", "strikeouts", "walks"]],
    on="player_id",
    how="left"
)

# Drop rows with missing values
df.dropna(subset=["strikeouts", "walks"], inplace=True)

# Compute z-scores (lower is better for ERA, WHIP, Walks; higher for Ks)
df["era_z"] = -zscore(df["era"])
df["whip_z"] = -zscore(df["whip"])
df["strikeouts_z"] = zscore(df["strikeouts"])
df["walks_z"] = -zscore(df["walks"])

# Composite mega_z
df["mega_z"] = df[["era_z", "whip_z", "strikeouts_z", "walks_z"]].mean(axis=1)

# Build prop rows
props = []

for _, row in df.iterrows():
    for prop_type, stat_value, lines in [
        ("strikeouts", row["strikeouts"], [4.5, 5.5, 6.5]),
        ("walks", row["walks"], [1.5, 2.5])
    ]:
        for line in lines:
            props.append({
                "player_id": row["player_id"],
                "name": row["name"],
                "team": row["team"],
                "prop_type": prop_type,
                "line": line,
                "value": stat_value,
                "z_score": row[f"{prop_type}_z"],
                "mega_z": row["mega_z"]
            })

# Convert to DataFrame and save
props_df = pd.DataFrame(props)
props_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}")
