#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from scipy.stats import zscore, norm

# File paths
INPUT_PROPS = Path("data/_projections/pitcher_props_projected.csv")
XTRA_STATS = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_mega_z.csv")

def pick_col(df: pd.DataFrame, candidates) -> str | None:
    """Return the first column name that exists in df from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    # try case-insensitive
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    return None

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

# Load data
df_base = pd.read_csv(INPUT_PROPS)
df_xtra = pd.read_csv(XTRA_STATS)

# Ensure consistent ID types
df_base["player_id"] = df_base["player_id"].astype(str).str.strip()
df_xtra["player_id"] = df_xtra["player_id"].astype(str).str.strip()

# ---- Ensure 'name' (and 'team' if missing) are available in base ----
if "name" not in df_base.columns or (df_base["name"].isna().all() if "name" in df_base.columns else True):
    cols_avail = [c for c in ["player_id", "name", "team"] if c in df_xtra.columns]
    if "player_id" in cols_avail and (("name" in cols_avail) or ("team" in cols_avail)):
        df_base = df_base.merge(
            df_xtra[cols_avail].drop_duplicates("player_id"),
            on="player_id",
            how="left",
            suffixes=("", "_xtra"),
        )
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

# ---- Find strikeouts / walks columns in xtra (robust) ----
k_col = pick_col(df_xtra, ["strikeouts", "k", "k_total", "k_count", "strike_outs"])
bb_col = pick_col(df_xtra, ["walks", "bb", "bb_total", "walk_count"])

if k_col is None or bb_col is None:
    # Print available columns to help debugging and exit gracefully
    print("❌ Required columns not found in pitchers_xtra_normalized.csv")
    print("   Need something like 'strikeouts' and 'walks'.")
    print("   Available columns:", list(df_xtra.columns))
    raise SystemExit(1)

# Merge on player_id pulling only the found columns
df = df_base.merge(
    df_xtra[["player_id", k_col, bb_col]],
    on="player_id",
    how="left"
).rename(columns={k_col: "strikeouts", bb_col: "walks"})

# Ensure ERA/WHIP exist (pull from xtra if needed)
if "era" not in df.columns and "era" in df_xtra.columns:
    df = df.merge(df_xtra[["player_id", "era"]], on="player_id", how="left")
if "whip" not in df.columns and "whip" in df_xtra.columns:
    df = df.merge(df_xtra[["player_id", "whip"]], on="player_id", how="left")

# Coerce numerics
for c in ["era", "whip", "strikeouts", "walks"]:
    if c in df.columns:
        df[c] = to_num(df[c])

# Drop rows missing strikeouts/walks after coercion
if not {"strikeouts", "walks"}.issubset(df.columns):
    missing = {"strikeouts", "walks"} - set(df.columns)
    print(f"❌ Missing expected columns after merge: {sorted(missing)}")
    raise SystemExit(1)

df = df.dropna(subset=["strikeouts", "walks"]).copy()

# If ERA/WHIP missing for some rows, compute z-scores on available subset
need_era_whip = df[["era", "whip"]].dropna().index
if len(need_era_whip) == 0:
    # If no ERA/WHIP present, set their z to 0 (neutral) to proceed
    df["era_z"] = 0.0
    df["whip_z"] = 0.0
else:
    # Compute z-scores only on rows with both values, fill others with 0
    era_z = -zscore(df.loc[need_era_whip, "era"])
    whip_z = -zscore
