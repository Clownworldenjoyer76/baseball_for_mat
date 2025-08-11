#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from scipy.stats import zscore, norm

# File paths
INPUT_PROPS = Path("data/_projections/pitcher_props_projected.csv")
XTRA_STATS  = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_mega_z.csv")

# ---- helpers ----------------------------------------------------------------
def pick_col(df: pd.DataFrame, candidates) -> str | None:
    """Return the first column name in df matching any candidate (case-insensitive)."""
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

def require_cols_or_die(df_label: str, df: pd.DataFrame, needed: list[str]):
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"❌ {df_label} missing required columns: {missing}")
        print(f"   Available columns: {list(df.columns)}")
        raise SystemExit(1)

# ---- load -------------------------------------------------------------------
df_base = pd.read_csv(INPUT_PROPS)
df_xtra = pd.read_csv(XTRA_STATS)

# ids consistent
df_base["player_id"] = df_base["player_id"].astype(str).str.strip()
df_xtra["player_id"]  = df_xtra["player_id"].astype(str).str.strip()

# ---- ensure name/team in base (fallback from xtra if needed) ----------------
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

# ---- locate strikeouts / walks columns (xtra OR base) -----------------------
K_CANDIDATES  = ["strikeouts","k","k_total","k_count","strike_outs","proj_k","k_pred","strikeouts_projected"]
BB_CANDIDATES = ["walks","bb","bb_total","walk_count","proj_bb","bb_pred","walks_projected"]

k_col_xtra  = pick_col(df_xtra, K_CANDIDATES)
bb_col_xtra = pick_col(df_xtra, BB_CANDIDATES)

k_col_base  = pick_col(df_base, K_CANDIDATES)
bb_col_base = pick_col(df_base, BB_CANDIDATES)

# Priority: xtra first, then base
k_source = ("xtra", k_col_xtra) if k_col_xtra else (("base", k_col_base) if k_col_base else (None, None))
bb_source = ("xtra", bb_col_xtra) if bb_col_xtra else (("base", bb_col_base) if bb_col_base else (None, None))

if k_source[0] is None or bb_source[0] is None:
    print("❌ Required strikeouts/walks not found in either file.")
    print("   Looked for K in:", K_CANDIDATES)
    print("   Looked for BB in:", BB_CANDIDATES)
    print("   Columns in pitchers_xtra_normalized.csv:", list(df_xtra.columns))
    print("   Columns in pitcher_props_projected.csv:", list(df_base.columns))
    raise SystemExit(1)

# ---- build working df with K/BB ---------------------------------------------
df = df_base.copy()

# bring K/BB from chosen sources
if k_source[0] == "xtra":
    df = df.merge(df_xtra[["player_id", k_source[1]]], on="player_id", how="left").rename(columns={k_source[1]:"strikeouts"})
else:
    df = df.rename(columns={k_source[1]:"strikeouts"})

if bb_source[0] == "xtra":
    df = df.merge(df_xtra[["player_id", bb_source[1]]], on="player_id", how="left").rename(columns={bb_source[1]:"walks"})
else:
    df = df.rename(columns={bb_source[1]:"walks"})

# ensure we now have columns
require_cols_or_die("Working DF", df, ["strikeouts","walks"])

# ---- ERA/WHIP sourcing (prefer base, else xtra) -----------------------------
if "era" not in df.columns and "era" in df_xtra.columns:
    df = df.merge(df_xtra[["player_id","era"]], on="player_id", how="left")
if "whip" not in df.columns and "whip" in df_xtra.columns:
    df = df.merge(df_xtra[["player_id","whip"]], on="player_id", how="left")

# coerce numerics
for c in ["era","whip","strikeouts","walks"]:
    if c in df.columns:
        df[c] = to_num(df[c])

# drop rows missing K/BB
df = df.dropna(subset=["strikeouts","walks"]).copy()
if df.empty:
    print("❌ After coercion, no rows have both strikeouts and walks.")
    raise SystemExit(1)

# ---- z-scores ----------------------------------------------------------------
# ERA/WHIP: compute z on rows where both exist; fill missing with 0
if "era" in df.columns and "whip" in df.columns:
    mask_e_w = df["era"].notna() & df["whip"].notna()
    df["era_z"]  = 0.0
    df["whip_z"] = 0.0
    if mask_e_w.any():
        df.loc[mask_e_w, "era_z"]  = -zscore(df.loc[mask_e_w, "era"])
        df.loc[mask_e_w, "whip_z"] = -zscore(df.loc[mask_e_w, "whip"])
else:
    df["era_z"] = 0.0
    df["whip_z"] = 0.0

df["strikeouts_z"] = zscore(df["strikeouts"].astype(float))
df["walks_z"]      = -zscore(df["walks"].astype(float))
df["mega_z"]       = df[["era_z","whip_z","strikeouts_z","walks_z"]].mean(axis=1)

# ---- build prop rows ---------------------------------------------------------
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

# ---- write -------------------------------------------------------------------
props_df = pd.DataFrame(props)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
props_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}")
