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

# ---------- Load source files ----------
pp = pd.read_csv(PITCHER_PROPS_IN)
pm = pd.read_csv(PITCHER_MEGA_IN)
bp = pd.read_csv(BATTER_PROJ_IN)
be = pd.read_csv(BATTER_EXP_IN)

tg = pd.read_csv(TODAYS_GAMES_IN)
stad = pd.read_csv(STADIUM_MASTER_IN)

# ---------- Normalize types / helpers ----------
def to_str(series):
    return series.astype("string").fillna(pd.NA)

def to_num(series):
    return pd.to_numeric(series, errors="coerce")

# Build STARters long form from todaysgames_normalized
# Columns available:
# game_id, home_team, away_team, game_time, pitcher_home, pitcher_away,
# home_team_id, away_team_id, pitcher_home_id, pitcher_away_id, park_factor

tg_cols_needed = [
    "game_id","home_team","away_team","pitcher_home_id","pitcher_away_id",
    "home_team_id","away_team_id","park_factor"
]
missing_tg = [c for c in tg_cols_needed if c not in tg.columns]
if missing_tg:
    raise ValueError(f"todaysgames_normalized is missing columns: {missing_tg}")

# Long starters with role
home = tg[["game_id","home_team_id","away_team_id","pitcher_home_id","park_factor"]].copy()
home["role"] = "HOME"
home.rename(columns={
    "home_team_id":"team_id",
    "away_team_id":"opponent_team_id",
    "pitcher_home_id":"player_id"
}, inplace=True)

away = tg[["game_id","home_team_id","away_team_id","pitcher_away_id","park_factor"]].copy()
away["role"] = "AWAY"
away.rename(columns={
    "away_team_id":"team_id",
    "home_team_id":"opponent_team_id",
    "pitcher_away_id":"player_id"
}, inplace=True)

starters = pd.concat([home, away], ignore_index=True)

# Coerce dtypes
starters["game_id"] = to_str(starters["game_id"])
starters["team_id"] = to_num(starters["team_id"]).astype("Int64")
starters["opponent_team_id"] = to_num(starters["opponent_team_id"]).astype("Int64")
starters["player_id_raw"] = starters["player_id"]
starters["player_id"] = to_str(starters["player_id"])
starters["park_factor"] = to_num(starters["park_factor"])

# Add venue fields via stadium_master (key: team_id)
# stadium_master columns: team_id,team_name,venue,city,state,timezone,is_dome,latitude,longitude,home_team
venue_cols = ["team_id","city","state","timezone","is_dome"]
missing_stad = [c for c in venue_cols if c not in stad.columns]
if missing_stad:
    raise ValueError(f"stadium_master is missing columns: {missing_stad}")
venue = stad[venue_cols].copy()
venue["team_id"] = to_num(venue["team_id"]).astype("Int64")

starters = starters.merge(venue, on="team_id", how="left")

# Handle UNDECIDED pitchers (NaN player_id)
is_undecided = starters["player_id"].isna()
if is_undecided.any():
    starters.loc[is_undecided, "player_id"] = (
        "UNDECIDED_" +
        starters.loc[is_undecided, "team_id"].astype("string") + "_" +
        starters.loc[is_undecided, "game_id"].astype("string") + "_" +
        starters.loc[is_undecided, "role"].astype("string")
    )
starters["undecided"] = is_undecided

# ---------- Fix 1: pitcher_props_projected → *_fixed ----------
pp = pp.copy()
# Ensure player_id exists and is string
if "player_id" not in pp.columns:
    raise ValueError("pitcher_props_projected.csv is missing 'player_id' column.")
pp["player_id"] = to_str(pp["player_id"])

# Attach starters context on player_id (left: keep pp rows, add context when found)
pp_ctx = pp.merge(
    starters.drop(columns=["player_id_raw"]),
    on="player_id",
    how="left",
    suffixes=("", "_ctx")
)

# Ensure the standardized context columns exist even if merge failed
context_defaults = {
    "game_id": pd.NA,
    "role": pd.NA,
    "team_id": pd.NA,
    "opponent_team_id": pd.NA,
    "park_factor": np.nan,
    "city": pd.NA,
    "state": pd.NA,
    "timezone": pd.NA,
    "is_dome": pd.NA,
    "undecided": False
}
for col, default in context_defaults.items():
    if col not in pp_ctx.columns:
        pp_ctx[col] = default

# Column order: preserve original pp columns, then append context fields
pp_ordered = pp_ctx.copy()
for col in ["game_id","role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome","undecided"]:
    # move to end if it exists
    if col in pp_ordered.columns:
        c = pp_ordered.pop(col)
        pp_ordered[col] = c

pp_ordered.to_csv(PITCHER_PROPS_OUT, index=False)

# ---------- Fix 2: pitcher_mega_z → *_fixed ----------
pm = pm.copy()
# Ensure player_id exists and string
if "player_id" not in pm.columns:
    raise ValueError("pitcher_mega_z.csv is missing 'player_id' column.")
pm["player_id"] = to_str(pm["player_id"])

# Map starters onto mega (adds flags and context for today's starters)
attach_cols = ["player_id","game_id","role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome","undecided"]
pm_ctx = pm.merge(
    starters[attach_cols].drop_duplicates("player_id"),
    on="player_id",
    how="left",
    suffixes=("", "_ctx")
)
pm_ctx["is_today"] = pm_ctx["game_id"].notna()

# Move appended context to the end
for col in ["is_today","game_id","role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome","undecided"]:
    c = pm_ctx.pop(col) if col in pm_ctx.columns else pd.Series(dtype="object")
    pm_ctx[col] = c

pm_ctx.to_csv(PITCHER_MEGA_OUT, index=False)

# ---------- Fix 3: batter_props_projected → *_fixed ----------
bp = bp.copy()
# Add empty game_id column (to be populated by a future context script)
if "game_id" not in bp.columns:
    bp["game_id"] = ""

# Ensure proj_hr_rate_pa_used exists (it already does here; keep as-is)
if "proj_hr_rate_pa_used" not in bp.columns:
    # If truly missing, create as NaN to avoid breakage
    bp["proj_hr_rate_pa_used"] = np.nan

# Create placeholder adjustment columns for future scripts (don’t compute here)
for col in ["adj_woba_weather","adj_woba_park","adj_woba_combined"]:
    if col not in bp.columns:
        bp[col] = np.nan

# Put placeholders at end
for col in ["game_id","adj_woba_weather","adj_woba_park","adj_woba_combined"]:
    c = bp.pop(col)
    bp[col] = c

bp.to_csv(BATTER_PROJ_OUT, index=False)

# ---------- Fix 4: batter_props_expanded → *_fixed ----------
be = be.copy()

# Add proj_hr_rate_pa_used by joining from bp on player_id
need_cols = ["player_id","proj_hr_rate_pa_used"]
if not all(c in bp.columns for c in need_cols):
    raise ValueError("batter_props_projected missing required columns for join into expanded.")

be = be.merge(
    bp[need_cols],
    on="player_id",
    how="left",
    suffixes=("", "_from_proj")
)

# Add empty game_id
if "game_id" not in be.columns:
    be["game_id"] = ""

# Add placeholder adjustment columns
for col in ["adj_woba_weather","adj_woba_park","adj_woba_combined"]:
    if col not in be.columns:
        be[col] = np.nan

# Move new columns to the end for consistency
for col in ["proj_hr_rate_pa_used","game_id","adj_woba_weather","adj_woba_park","adj_woba_combined"]:
    c = be.pop(col)
    be[col] = c

be.to_csv(BATTER_EXP_OUT, index=False)

# ---------- Simple console summary ----------
def count_na(df, cols):
    return {c: int(df[c].isna().sum()) if c in df.columns else None for c in cols}

print("✔ Wrote:", PITCHER_PROPS_OUT, "rows=", len(pp_ordered))
print("   ctx NAs:", count_na(pp_ordered, ["game_id","team_id","park_factor","city","state","timezone","is_dome"]))
print("✔ Wrote:", PITCHER_MEGA_OUT, "rows=", len(pm_ctx), "is_today=", int(pm_ctx["is_today"].sum()))
print("✔ Wrote:", BATTER_PROJ_OUT, "rows=", len(bp))
print("✔ Wrote:", BATTER_EXP_OUT, "rows=", len(be))
