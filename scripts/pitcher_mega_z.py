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

# Normalize names
df_xtra["last_name, first_name"] = df_xtra["last_name, first_name"].astype(str).str.strip()
df_base["name"] = df_base["name"].astype(str).str.strip()

# Merge by name
df = df_base.merge(
    df_xtra[["last_name, first_name", "strikeouts", "walks"]],
    left_on="name",
    right_on="last_name, first_name",
    how="left"
).drop(columns=["last_name, first_name"])

# Z-score components
df["era_z"] = -zscore(df["era"])
df["whip_z"] = -zscore(df["whip"])
df["strikeouts_z"] = zscore(df["strikeouts"])
df["walks_z"] = -zscore(df["walks"])

# Composite score
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

# Create output DataFrame
props_df = pd.DataFrame(props)

# Save to CSV
props_df.to_csv(OUTPUT_FILE, index=False)
