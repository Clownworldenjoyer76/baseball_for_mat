import pandas as pd
from pathlib import Path
from scipy.stats import zscore, norm

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


# ---- Ensure 'name' (and 'team' if missing) are available in base ----
if "name" not in df_base.columns or df_base["name"].isna().all() if "name" in df_base.columns else True:
    # Try to bring from xtra
    cols_avail = [c for c in ["player_id","name","team"] if c in df_xtra.columns]
    if "player_id" in cols_avail and ("name" in cols_avail or "team" in cols_avail):
        df_base = df_base.merge(df_xtra[cols_avail].drop_duplicates("player_id"), on="player_id", how="left", suffixes=("", "_xtra"))
        # Prefer base non-null values where present
        if "name_xtra" in df_base.columns:
            if "name" in df_base.columns:
                df_base["name"] = df_base["name"].fillna(df_base["name_xtra"])
            else:
                df_base["name"] = df_base["name_xtra"]
            df_base.drop(columns=["name_xtra"], inplace=True)
        if "team_xtra" in df_base.columns:
            if "team" in df_base.columns:
                df_base["team"] = df_base["team"].fillna(df_base["team_xtra"])
            else:
                df_base["team"] = df_base["team_xtra"]
            df_base.drop(columns=["team_xtra"], inplace=True)

# Fallback: build name from 'last_name, first_name' if still missing
if "name" not in df_base.columns and "last_name, first_name" in df_base.columns:
    df_base["name"] = df_base["last_name, first_name"]


# Merge on player_id
df = df_base.merge(
    df_xtra[["player_id", "strikeouts", "walks"]],
    on="player_id",
    how="left"
)

# Drop rows with missing values
df.dropna(subset=["strikeouts", "walks"], inplace=True)

# Compute z-scores
df["era_z"] = -zscore(df["era"])
df["whip_z"] = -zscore(df["whip"])
df["strikeouts_z"] = zscore(df["strikeouts"])
df["walks_z"] = -zscore(df["walks"])
df["mega_z"] = df[["era_z", "whip_z", "strikeouts_z", "walks_z"]].mean(axis=1)

# Build prop rows
props = []
for _, row in df.iterrows():
    for prop_type, stat_value, lines in [
        ("strikeouts", row["strikeouts"], [4.5, 5.5, 6.5]),
        ("walks", row["walks"], [1.5, 2.5])
    ]:
        for line in lines:
            z = row[f"{prop_type}_z"]
            props.append({
                "player_id": row["player_id"],
                "name": row["name"],
                "team": row["team"],
                "prop_type": prop_type,
                "line": line,
                "value": stat_value,
                "z_score": round(z, 4),
                "mega_z": round(row["mega_z"], 4),
                "over_probability": round(1 - norm.cdf(z), 4)
            })

# Convert to DataFrame and save
props_df = pd.DataFrame(props)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
props_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}")
