import pandas as pd
from pathlib import Path
from scipy.stats import zscore

# File paths
PROPS_PATH = Path("data/_projections/pitcher_props_projected.csv")
XTRA_PATH = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_PATH = Path("data/_projections/pitcher_mega_z.csv")

def load_and_merge():
    props = pd.read_csv(PROPS_PATH)
    xtra = pd.read_csv(XTRA_PATH)

    props["player_id"] = props["player_id"].astype(str).str.strip()
    xtra["player_id"] = xtra["player_id"].astype(str).str.strip()

    df = props.merge(xtra, on="player_id", how="inner", suffixes=("_x", "_y"))
    return df

def compute_z(df):
    # Select relevant stat columns to z-score
    stat_cols = [
        "strikeouts", 
        "walks", 
        "innings_pitched", 
        "earned_runs", 
        "era", 
        "whip"
    ]

    # Drop rows with missing required data
    df_clean = df.dropna(subset=stat_cols).copy()

    # Normalize relevant stats
    for col in stat_cols:
        df_clean[col + "_z"] = zscore(df_clean[col])

    # Compute composite z-score (lower ERA/WHIP/ER/BB = better; higher IP/K = better)
    df_clean["composite_z"] = (
        df_clean["strikeouts_z"] +
        df_clean["innings_pitched_z"] -
        df_clean["earned_runs_z"] -
        df_clean["walks_z"] -
        df_clean["era_z"] -
        df_clean["whip_z"]
    )

    return df_clean

def export(df):
    out = df[[
        "player_id",
        "name_x",
        "team_x",
        "composite_z"
    ]].rename(columns={
        "name_x": "name",
        "team_x": "team",
        "composite_z": "z_score"
    })

    out = out.sort_values(by="z_score", ascending=False).reset_index(drop=True)
    out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Exported: {OUTPUT_PATH}")

if __name__ == "__main__":
    df_merged = load_and_merge()
    df_scored = compute_z(df_merged)
    export(df_scored)
