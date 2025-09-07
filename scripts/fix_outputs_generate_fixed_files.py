# scripts/fix_outputs_generate_fixed_files.py
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

# ---------- Helpers ----------
def to_str(s):
    return s.astype(str).str.strip()

def to_int64(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def read_csv(path):
    return pd.read_csv(path) if Path(path).exists() else pd.DataFrame()

# ---------- Load inputs ----------
pp = read_csv(PITCHER_PROPS_IN)      # may be incomplete
pm = read_csv(PITCHER_MEGA_IN)
bp = read_csv(BATTER_PROJ_IN)
be = read_csv(BATTER_EXP_IN)
tg = read_csv(TODAYS_GAMES_IN)
stad = read_csv(STADIUM_MASTER_IN)

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
starters["player_id"] = to_int64(starters["player_id"])
starters["team_id"] = to_int64(starters["team_id"])
starters["opponent_team_id"] = to_int64(starters["opponent_team_id"])
starters["game_id"] = to_int64(starters["game_id"])
starters["park_factor"] = pd.to_numeric(starters["park_factor"], errors="coerce")

# Optional park context (if you maintain stadium master)
ctx_cols = ["team_id","city","state","timezone","is_dome"]
if not stad.empty and all(c in stad.columns for c in ctx_cols):
    stad_ctx = stad[ctx_cols].copy()
    stad_ctx["team_id"] = to_int64(stad_ctx["team_id"])
    starters = starters.merge(stad_ctx, on="team_id", how="left")
else:
    starters["city"] = pd.NA
    starters["state"] = pd.NA
    starters["timezone"] = pd.NA
    starters["is_dome"] = pd.NA

# ---------- Normalize projections (pp) ----------
if not pp.empty:
    # Ensure consistent dtypes
    if "player_id" not in pp.columns:
        raise ValueError("pitcher_props_projected.csv is missing 'player_id'")
    pp["player_id"] = to_int64(pp["player_id"])
    numeric_like = [c for c in pp.columns if c.startswith("proj_") or c.endswith("_percent") or c in
                    ["adj_woba_weather","adj_woba_park","adj_woba_combined","proj_hits","proj_hr",
                     "proj_avg","proj_slg","k_percent_eff","bb_percent_eff","pa","ab","innings_pitched"]]
    for c in numeric_like:
        if c in pp.columns:
            pp[c] = pd.to_numeric(pp[c], errors="coerce")
else:
    pp = pd.DataFrame(columns=["player_id","proj_hits","proj_hr","proj_avg","proj_slg","k_percent_eff","bb_percent_eff",
                               "adj_woba_weather","adj_woba_park","adj_woba_combined","pa","ab","innings_pitched"])

# ---------- NEW: Left-join projections onto starters (keep ALL starters) ----------
pp_ctx = starters.merge(pp, on="player_id", how="left", suffixes=("", "_pp"))

# Ensure required columns exist (don’t fabricate values; allow NaN)
for c in ["proj_hits","proj_hr","proj_avg","proj_slg","k_percent_eff","bb_percent_eff",
          "adj_woba_weather","adj_woba_park","adj_woba_combined","pa","ab","innings_pitched"]:
    if c not in pp_ctx.columns:
        pp_ctx[c] = np.nan

# Order: keep projections first-ish, then context
ordered = ["player_id","proj_hits","proj_hr","proj_avg","proj_slg","k_percent_eff","bb_percent_eff",
           "adj_woba_weather","adj_woba_park","adj_woba_combined",
           "role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome",
           "game_id","pa","ab","innings_pitched"]
pp_ctx = pp_ctx[[c for c in ordered if c in pp_ctx.columns] + [c for c in pp_ctx.columns if c not in ordered]]

pp_ctx.to_csv(PITCHER_PROPS_OUT, index=False)

# ---------- Map starters onto mega-z for convenience ----------
if not pm.empty:
    pm = pm.copy()
    if "player_id" not in pm.columns:
        raise ValueError("pitcher_mega_z.csv is missing 'player_id'")
    pm["player_id"] = to_int64(pm["player_id"])
    attach = starters[["player_id","game_id","role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome"]].drop_duplicates("player_id")
    pm_ctx = pm.merge(attach, on="player_id", how="left")
else:
    pm_ctx = pm
pm_ctx.to_csv(PITCHER_MEGA_OUT, index=False)

# ---------- Pass-through for batters with header hygiene ----------
def clean_headers(df):
    return df.rename(columns=lambda c: c.strip())

if not bp.empty:
    bp = clean_headers(bp)
    bp.to_csv(BATTER_PROJ_OUT, index=False)
if not be.empty:
    be = clean_headers(be)
    be.to_csv(BATTER_EXP_OUT, index=False)

# ---------- Console summary ----------
print("✔ Wrote:", PITCHER_PROPS_OUT, "rows=", len(pp_ctx))
print("✔ Wrote:", PITCHER_MEGA_OUT, "rows=", len(pm_ctx))
print("✔ Wrote:", BATTER_PROJ_OUT, "rows=", len(bp))
print("✔ Wrote:", BATTER_EXP_OUT, "rows=", len(be))
