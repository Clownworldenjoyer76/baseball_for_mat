#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from scipy.stats import zscore, norm
import re

# File paths
INPUT_PROPS = Path("data/_projections/pitcher_props_projected.csv")
XTRA_STATS  = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_mega_z.csv")

# ---------- helpers ----------
def pick_col(df: pd.DataFrame, candidates) -> str | None:
    cols = list(df.columns)
    low = {c.lower(): c for c in cols}
    for c in candidates:
        if c in cols:
            return c
        lc = c.lower()
        if lc in low:
            return low[lc]
    return None

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def coalesce_stat(df: pd.DataFrame, target: str, candidates: list[str]) -> pd.DataFrame:
    """
    Create/overwrite df[target] by coalescing the first non-null from:
      1) exact target if present
      2) any candidate columns (case-insensitive)
      3) any columns that match ^{target}(_.*)?$  (e.g., strikeouts_x, strikeouts_y)
    Drops the helper columns after coalescing.
    """
    all_cols = list(df.columns)
    chosen = []
    # exact first
    if target in df.columns:
        chosen.append(target)
    # explicit candidates
    low = {c.lower(): c for c in all_cols}
    for c in candidates:
        if c in df.columns:
            chosen.append(c)
        elif c.lower() in low and low[c.lower()] not in chosen:
            chosen.append(low[c.lower()])
    # suffix variants
    pat = re.compile(rf"^{re.escape(target)}(_.*)?$", re.IGNORECASE)
    for c in all_cols:
        if pat.match(c) and c not in chosen:
            chosen.append(c)

    if not chosen:
        return df  # nothing to do

    # build the coalesced series
    ser = None
    for c in chosen:
        if ser is None:
            ser = df[c]
        else:
            ser = ser.where(ser.notna(), df[c])

    df[target] = ser
    # drop everything except the final target
    drop_cols = [c for c in chosen if c != target]
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True, errors="ignore")
    return df

# ---------- load ----------
df_base = pd.read_csv(INPUT_PROPS)
df_xtra = pd.read_csv(XTRA_STATS)

# ids consistent
df_base["player_id"] = df_base["player_id"].astype(str).str.strip()
df_xtra["player_id"]  = df_xtra["player_id"].astype(str).str.strip()

# ensure name/team present (fallback from xtra)
if "name" not in df_base.columns or (df_base["name"].isna().all() if "name" in df_base.columns else True):
    cols_avail = [c for c in ["player_id","name","team"] if c in df_xtra.columns]
    if "player_id" in cols_avail and (("name" in cols_avail) or ("team" in cols_avail)):
        df_base = df_base.merge(
            df_xtra[cols_avail].drop_duplicates("player_id"),
            on="player_id",
            how="left",
            suffixes=("", "_xtra"),
        )
        if "name_xtra" in df_base.columns:
            df_base["name"] = df_base.get("name", pd.Series(index=df_base.index)).fillna(df_base["name_xtra"])
            df_base.drop(columns=["name_xtra"], inplace=True)
        if "team_xtra" in df_base.columns:
            df_base["team"] = df_base.get("team", pd.Series(index=df_base.index)).fillna(df_base["team_xtra"])
            df_base.drop(columns=["team_xtra"], inplace=True)

if "name" not in df_base.columns and "last_name, first_name" in df_base.columns:
    df_base["name"] = df_base["last_name, first_name"]

# ---------- bring K/BB from xtra if present ----------
K_CANDS  = ["strikeouts","k","k_total","k_count","strike_outs","proj_k","k_pred","strikeouts_projected"]
BB_CANDS = ["walks","bb","bb_total","walk_count","proj_bb","bb_pred","walks_projected"]

use_xtra_k  = pick_col(df_xtra, K_CANDS)
use_xtra_bb = pick_col(df_xtra, BB_CANDS)

df = df_base.copy()
if use_xtra_k:
    df = df.merge(df_xtra[["player_id", use_xtra_k]], on="player_id", how="left")
if use_xtra_bb:
    df = df.merge(df_xtra[["player_id", use_xtra_bb]], on="player_id", how="left")

# also coalesce any K/BB already in base (handles *_x/*_y)
df = coalesce_stat(df, "strikeouts", K_CANDS + ([use_xtra_k] if use_xtra_k else []))
df = coalesce_stat(df, "walks",      BB_CANDS + ([use_xtra_bb] if use_xtra_bb else []))

# require present after coalescing
if not {"strikeouts","walks"}.issubset(df.columns):
    print("❌ Working DF missing required columns: ['strikeouts','walks']")
    print("   Available columns:", list(df.columns))
    raise SystemExit(1)

# ---------- ERA/WHIP (prefer base, else xtra; then coalesce suffixes) ----------
if "era" not in df.columns and "era" in df_xtra.columns:
    df = df.merge(df_xtra[["player_id","era"]], on="player_id", how="left")
if "whip" not in df.columns and "whip" in df_xtra.columns:
    df = df.merge(df_xtra[["player_id","whip"]], on="player_id", how="left")

df = coalesce_stat(df, "era",  ["ERA"])
df = coalesce_stat(df, "whip", ["WHIP"])

# ---------- numerics / pruning ----------
for c in ["era","whip","strikeouts","walks"]:
    if c in df.columns:
        df[c] = to_num(df[c])

# drop rows missing K/BB
df = df.dropna(subset=["strikeouts","walks"]).copy()
if df.empty:
    print("❌ After coercion, no rows have both strikeouts and walks.")
    raise SystemExit(1)

# ---------- z-scores ----------
# ERA/WHIP: compute z where both exist; else 0
df["era_z"]  = 0.0
df["whip_z"] = 0.0
mask_e_w = df["era"].notna() & df["whip"].notna()
if mask_e_w.any():
    df.loc[mask_e_w, "era_z"]  = -zscore(df.loc[mask_e_w, "era"])
    df.loc[mask_e_w, "whip_z"] = -zscore(df.loc[mask_e_w, "whip"])

df["strikeouts_z"] = zscore(df["strikeouts"].astype(float))
df["walks_z"]      = -zscore(df["walks"].astype(float))
df["mega_z"]       = df[["era_z","whip_z","strikeouts_z","walks_z"]].mean(axis=1)

# ---------- props ----------
props = []
for _, row in df.iterrows():
    for prop_type, stat_value, lines in [
        ("strikeouts", row["strikeouts"], [4.5, 5.5, 6.5]),
        ("walks",      row["walks"],      [1.5, 2.5]),
    ]:
        z = float(row[f"{prop_type}_z"])
        over_prob = float(1 - norm.cdf(z))
        for line in lines:
            props.append({
                "player_id": row.get("player_id"),
                "name":      row.get("name"),
                "team":      row.get("team"),
                "prop_type": prop_type,
                "line":      line,
                "value":     float(stat_value) if pd.notna(stat_value) else None,
                "z_score":   round(z, 4),
                "mega_z":    round(float(row["mega_z"]), 4),
                "over_probability": round(over_prob, 4),
            })

# ---------- write ----------
props_df = pd.DataFrame(props)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
props_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}")
