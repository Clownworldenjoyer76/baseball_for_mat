#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

# ---------- Paths ----------
PITCHER_PROPS_IN   = "data/_projections/pitcher_props_projected.csv"
PITCHER_MEGA_IN    = "data/_projections/pitcher_mega_z.csv"
BATTER_PROJ_IN     = "data/_projections/batter_props_projected.csv"
BATTER_EXP_IN      = "data/_projections/batter_props_expanded.csv"

TODAYS_GAMES_IN    = "data/raw/todaysgames_normalized.csv"
STADIUM_MASTER_IN  = "data/manual/stadium_master.csv"

PITCHER_PROPS_OUT  = "data/_projections/pitcher_props_projected_fixed.csv"
PITCHER_MEGA_OUT   = "data/_projections/pitcher_mega_z_fixed.csv"
BATTER_PROJ_OUT    = "data/_projections/batter_props_projected_fixed.csv"
BATTER_EXP_OUT     = "data/_projections/batter_props_expanded_fixed.csv"

Path("data/_projections").mkdir(parents=True, exist_ok=True)
Path("summaries/projections").mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------
def to_int64(s): return pd.to_numeric(s, errors="coerce").astype("Int64")
def read_csv(path): return pd.read_csv(path) if Path(path).exists() else pd.DataFrame()
def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    return df.rename(columns=lambda c: str(c).strip())

def num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

# ---------- Load inputs ----------
pp   = clean_headers(read_csv(PITCHER_PROPS_IN))      # pitcher projections (may include context)
pm   = clean_headers(read_csv(PITCHER_MEGA_IN))
bp   = clean_headers(read_csv(BATTER_PROJ_IN))        # batter projected (may NOT have game_id)
be   = clean_headers(read_csv(BATTER_EXP_IN))         # batter expanded (may NOT have game_id)
tg   = clean_headers(read_csv(TODAYS_GAMES_IN))
stad = clean_headers(read_csv(STADIUM_MASTER_IN))

# ---------- Validate schedule essentials ----------
need_tg = ["game_id","home_team_id","away_team_id","pitcher_home_id","pitcher_away_id","park_factor"]
miss = [c for c in need_tg if c not in tg.columns]
if miss:
    raise ValueError(f"todaysgames_normalized is missing columns: {miss}")

# ---------- Build authoritative starters list (BASE) ----------
home = tg[["game_id","home_team_id","away_team_id","pitcher_home_id","park_factor"]].copy()
home["role"] = "HOME"
home.rename(columns={
    "home_team_id": "team_id",
    "away_team_id": "opponent_team_id",
    "pitcher_home_id": "player_id"
}, inplace=True)

away = tg[["game_id","home_team_id","away_team_id","pitcher_away_id","park_factor"]].copy()
away["role"] = "AWAY"
away.rename(columns={
    "away_team_id": "team_id",
    "home_team_id": "opponent_team_id",
    "pitcher_away_id": "player_id"
}, inplace=True)

starters = pd.concat([home, away], ignore_index=True)
for c in ["player_id","team_id","opponent_team_id","game_id"]:
    starters[c] = to_int64(starters[c])
starters["park_factor"] = pd.to_numeric(starters["park_factor"], errors="coerce")

# Optional park context (if provided)
ctx_cols = ["team_id","city","state","timezone","is_dome"]
if not stad.empty and all(c in stad.columns for c in ctx_cols):
    stad_ctx = stad[ctx_cols].copy()
    stad_ctx["team_id"] = to_int64(stad_ctx["team_id"])
    starters = starters.merge(stad_ctx, on="team_id", how="left")
else:
    starters["city"]     = pd.NA
    starters["state"]    = pd.NA
    starters["timezone"] = pd.NA
    starters["is_dome"]  = pd.NA

# ---------- Normalize pitcher projections (pp) ----------
# Keep only stat fields from pp (NO context), as requested.
# Whitelist what we’re willing to merge from pp:
pp_stat_whitelist = [
    "player_id",
    "adj_woba_weather", "adj_woba_park", "adj_woba_combined",
    "proj_hits", "proj_hr", "proj_avg", "proj_slg",
    "k_percent_eff", "bb_percent_eff",
    "innings_pitched", "pa", "ab"
]
if not pp.empty:
    if "player_id" not in pp.columns:
        raise ValueError("pitcher_props_projected.csv is missing 'player_id'")
    # Cast types
    pp["player_id"] = to_int64(pp["player_id"])
    num(pp, ["adj_woba_weather","adj_woba_park","adj_woba_combined",
             "proj_hits","proj_hr","proj_avg","proj_slg",
             "k_percent_eff","bb_percent_eff","innings_pitched","pa","ab"])
    # Select intersection of whitelist and actual columns
    keep_cols = [c for c in pp_stat_whitelist if c in pp.columns]
    pp_stats = pp[keep_cols].copy()
else:
    pp_stats = pd.DataFrame(columns=pp_stat_whitelist)

# --- CRITICAL COLLISION GUARD ---
# If this script re-runs in the same workspace, ensure no leftover *_pp cols remain.
starters = starters.drop(columns=starters.filter(regex=r'_pp$').columns, errors='ignore')

# ---------- Left-join stats from pp onto starters (keep ALL starters) ----------
pp_ctx = starters.merge(pp_stats, on="player_id", how="left", suffixes=("", "_pp"))

# Order common fields (others, if any, stay at the end)
ordered = [
    "player_id",
    "proj_hits","proj_hr","proj_avg","proj_slg","k_percent_eff","bb_percent_eff",
    "adj_woba_weather","adj_woba_park","adj_woba_combined",
    "role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome",
    "game_id","pa","ab","innings_pitched"
]
pp_ctx = pp_ctx[[c for c in ordered if c in pp_ctx.columns] +
                [c for c in pp_ctx.columns if c not in ordered]]

pp_ctx.to_csv(PITCHER_PROPS_OUT, index=False)

# ---------- Map starters onto mega-z for convenience ----------
if not pm.empty:
    if "player_id" not in pm.columns:
        raise ValueError("pitcher_mega_z.csv is missing 'player_id'")
    pm = pm.copy()
    pm["player_id"] = to_int64(pm["player_id"])
    attach = starters[["player_id","game_id","role","team_id","opponent_team_id",
                       "park_factor","city","state","timezone","is_dome"]].drop_duplicates("player_id")
    pm_ctx = pm.merge(attach, on="player_id", how="left")
else:
    pm_ctx = pm
pm_ctx.to_csv(PITCHER_MEGA_OUT, index=False)

# ---------- Batter projected: pass-through (header hygiene + key dtype) ----------
if not bp.empty:
    bp = bp.copy()
    if "player_id" not in bp.columns:
        raise ValueError("batter_props_projected.csv missing 'player_id'")
    bp["player_id"] = to_int64(bp["player_id"])
    if "game_id" in bp.columns:
        bp["game_id"] = to_int64(bp["game_id"])
    bp_out = bp
else:
    bp_out = bp
bp_out.to_csv(BATTER_PROJ_OUT, index=False)

# ---------- Batter expanded: copy then sync *_pa_used from projected ----------
if not be.empty:
    be = be.copy()
    if "player_id" not in be.columns:
        raise ValueError("batter_props_expanded.csv missing 'player_id'")
    be["player_id"] = to_int64(be["player_id"])
    if "game_id" in be.columns:
        be["game_id"] = to_int64(be["game_id"])
    be_out = be

    pa_used_cols = [c for c in bp_out.columns if c.endswith("_pa_used")]
    to_add = [c for c in pa_used_cols if c not in be_out.columns]

    if to_add:
        # Join by (player_id, game_id) only if BOTH files have game_id; else fall back to player_id.
        if ("game_id" in bp_out.columns) and ("game_id" in be_out.columns):
            add_df = bp_out[["player_id","game_id"] + to_add].copy()
            add_df["player_id"] = to_int64(add_df["player_id"])
            add_df["game_id"]   = to_int64(add_df["game_id"])
            be_out = be_out.merge(add_df, on=["player_id","game_id"], how="left")
            join_mode = "player_id+game_id"
        else:
            add_df = bp_out[["player_id"] + to_add].copy()
            add_df["player_id"] = to_int64(add_df["player_id"])
            be_out = be_out.merge(add_df, on=["player_id"], how="left")
            join_mode = "player_id_only"

        with open("summaries/projections/batter_pa_used_sync.txt", "w", encoding="utf-8") as fh:
            fh.write(f"added_from_projected: {to_add}\n")
            fh.write(f"join_mode: {join_mode}\n")
    else:
        with open("summaries/projections/batter_pa_used_sync.txt", "w", encoding="utf-8") as fh:
            fh.write("added_from_projected: []\njoin_mode: n/a\n")
else:
    be_out = be

be_out.to_csv(BATTER_EXP_OUT, index=False)

# ---------- Console summary ----------
print("✔ Wrote:", PITCHER_PROPS_OUT, "rows=", len(pp_ctx))
print("✔ Wrote:", PITCHER_MEGA_OUT, "rows=", len(pm_ctx))
print("✔ Wrote:", BATTER_PROJ_OUT, "rows=", len(bp_out))
print("✔ Wrote:", BATTER_EXP_OUT, "rows=", len(be_out))
