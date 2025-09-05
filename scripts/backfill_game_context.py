#!/usr/bin/env python3
"""
backfill_game_context.py

Tasks
1) Validate the four *_fixed.csv outputs exist and have the required columns.
2) For pitcher_props_projected_fixed.csv, backfill opponent_team_id using
   data/end_chain/final/startingpitchers.csv (robust to multiple schemas).
3) Normalize 'undecided' to False for rows with a concrete pitcher_id.
4) Re-validate and write the updated CSVs in place.
5) Exit non-zero if any validation fails.

This script is idempotent and safe to run at the end of the pipeline.
"""

from __future__ import annotations
import sys
import os
import pandas as pd

# ---- Paths ----
PITCHER_PROPS_FIXED = "data/_projections/pitcher_props_projected_fixed.csv"
BATTER_PROJ_FIXED   = "data/_projections/batter_props_projected_fixed.csv"
BATTER_EXP_FIXED    = "data/_projections/batter_props_expanded_fixed.csv"
PITCHER_MEGAZ_FIXED = "data/_projections/pitcher_mega_z_fixed.csv"
STARTING_PITCHERS   = "data/end_chain/final/startingpitchers.csv"

# ---- Required columns (minimum viable) ----
REQUIRED = {
    BATTER_PROJ_FIXED: {
        "player_id", "name", "team",
        "prob_hits_over_1p5", "prob_tb_over_1p5", "prob_hr_over_0p5",
        "proj_pa_used", "proj_ab_est", "proj_avg_used", "proj_iso_used",
        "proj_hr_rate_pa_used",        # must be present/standardized
        "game_id",                     # placeholder column must exist (may be empty now)
        "adj_woba_weather", "adj_woba_park", "adj_woba_combined"  # placeholders for future append
    },
    BATTER_EXP_FIXED: {
        "player_id", "name", "team",
        "prob_hits_over_1p5", "prob_tb_over_1p5", "prob_hr_over_0p5",
        "proj_pa_used", "proj_ab_est", "proj_avg_used", "proj_iso_used",
        "proj_hr_rate_pa_used",        # synced with projected
        "game_id",
        "adj_woba_weather", "adj_woba_park", "adj_woba_combined"
    },
    PITCHER_PROPS_FIXED: {
        "player_id",
        "game_id", "role", "team_id",
        "opponent_team_id",            # we will fill if blank
        "undecided",                   # we will coerce False if pitcher is set
        # keep these for context (already present in your sample)
        "park_factor", "city", "state", "timezone", "is_dome"
    },
    # Keep pitcher_mega_z validation lightweight to avoid false failures
    PITCHER_MEGAZ_FIXED: {"player_id"}  # at least the key
}

def err(msg: str) -> None:
    print(f"❌ backfill_game_context: {msg}", file=sys.stderr)

def ok(msg: str) -> None:
    print(f"✅ backfill_game_context: {msg}")

def warn(msg: str) -> None:
    print(f"⚠️ backfill_game_context: {msg}")

def ensure_exists(path: str) -> None:
    if not os.path.exists(path):
        err(f"Missing file: {path}")
        raise FileNotFoundError(path)

def load_csv(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception as e:
        err(f"Failed to read CSV: {path} ({e})")
        raise
    return df

def validate_columns(df: pd.DataFrame, required: set[str], path: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        err(f"{path} missing required columns: {missing}")
        raise ValueError(f"Missing required columns in {path}: {missing}")

def to_lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def coerce_bool(series: pd.Series) -> pd.Series:
    # Convert many truthy/falsy representations safely
    return series.map(lambda x: str(x).strip().lower() in {"true", "1", "yes", "y"} if pd.notna(x) else False)

def build_game_map(df_sp: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize startingpitchers into columns: game_id, home_team_id, away_team_id
    Handles a few likely schemas.
    """
    cols = {c.lower(): c for c in df_sp.columns}
    # Prefer explicit home/away team id columns if present
    has_home = any(k in cols for k in ["home_team_id", "home_id", "home"])
    has_away = any(k in cols for k in ["away_team_id", "away_id", "away"])
    if "game_id" not in cols:
        raise ValueError("startingpitchers.csv must contain game_id")

    gi = cols["game_id"]
    df = df_sp.copy()

    def first_present(*cands):
        for k in cands:
            if k in cols:
                return cols[k]
        return None

    if has_home and has_away:
        home_col = first_present("home_team_id", "home_id", "home")
        away_col = first_present("away_team_id", "away_id", "away")
        out = df[[gi, home_col, away_col]].rename(columns={
            gi: "game_id",
            home_col: "home_team_id",
            away_col: "away_team_id"
        })
        return out

    # If we only have team/opponent per row with role, pivot it
    team_col = first_present("team_id", "team")
    opp_col  = first_present("opponent_team_id", "opponent_team", "opponent")
    role_col = first_present("role", "team_context", "home_away", "homeaway")

    if team_col and opp_col and role_col:
        df_min = df[[gi, team_col, opp_col, role_col]].rename(columns={
            gi: "game_id",
            team_col: "team_id",
            opp_col: "opponent_team_id",
            role_col: "role"
        })
        df_min["role"] = df_min["role"].str.upper().str.strip()
        # Build home/away mapping per game_id
        # If multiple rows per game, pick one HOME and one AWAY
        home_map = df_min.loc[df_min["role"] == "HOME", ["game_id", "team_id", "opponent_team_id"]]\
                          .rename(columns={"team_id": "home_team_id", "opponent_team_id": "away_team_id"})
        away_map = df_min.loc[df_min["role"] == "AWAY", ["game_id", "team_id", "opponent_team_id"]]\
                          .rename(columns={"team_id": "away_team_id", "opponent_team_id": "home_team_id"})
        # Merge and coalesce
        out = pd.merge(home_map, away_map, on="game_id", how="outer", suffixes=("_home_row","_away_row"))
        # Coalesce duplicates
        out["home_team_id"] = out["home_team_id_home_row"].fillna(out["home_team_id_away_row"])
        out["away_team_id"] = out["away_team_id_home_row"].fillna(out["away_team_id_away_row"])
        out = out[["game_id", "home_team_id", "away_team_id"]]
        return out

    # If we get here, we can't infer both sides; allow graceful warn.
    raise ValueError("Unable to infer (home_team_id, away_team_id) from startingpitchers.csv schema")

def backfill_pitcher_props():
    ensure_exists(PITCHER_PROPS_FIXED)
    ensure_exists(STARTING_PITCHERS)

    dfp = load_csv(PITCHER_PROPS_FIXED)
    dfp = to_lower_cols(dfp)  # keep original names (case preserved), but strip whitespace
    # Re-read with original casing preserved for write-back:
    dfp_orig = load_csv(PITCHER_PROPS_FIXED)

    # Basic validation first (using original column names)
    validate_columns(dfp_orig, REQUIRED[PITCHER_PROPS_FIXED], PITCHER_PROPS_FIXED)

    # Prepare role and game_id from original to avoid column case issues
    if "role" not in dfp_orig.columns or "game_id" not in dfp_orig.columns:
        raise ValueError("pitcher_props_projected_fixed.csv must have 'role' and 'game_id' columns")

    # Load starting pitchers and construct game map
    dfs = load_csv(STARTING_PITCHERS)
    dfs = to_lower_cols(dfs)

    try:
        game_map = build_game_map(dfs)
    except Exception as e:
        warn(f"Could not build game map from startingpitchers.csv: {e}")
        raise

    # Merge opponent team by role
    # We will compute desired opponent per row, but only fill when blank/NaN.
    # Create lookup dicts
    gm_home = game_map.set_index("game_id")["home_team_id"].to_dict()
    gm_away = game_map.set_index("game_id")["away_team_id"].to_dict()

    # Work on original frame to preserve column order
    df = dfp_orig.copy()

    # Normalize role
    df["role"] = df["role"].astype(str).str.upper().str.strip()
    # Prepare a target series
    opp_filled = df.get("opponent_team_id")

    # Compute preferred opponent from game map
    def infer_opp(row):
        gid = row.get("game_id")
        role = row.get("role")
        if pd.isna(gid) or pd.isna(role):
            return None
        try:
            gid_int = int(gid)
        except Exception:
            # leave as-is if not numeric
            gid_int = gid
        if role == "HOME":
            return gm_away.get(gid_int, gm_away.get(gid))
        elif role == "AWAY":
            return gm_home.get(gid_int, gm_home.get(gid))
        return None

    inferred = df.apply(infer_opp, axis=1)

    # Fill only blanks/NaN
    if "opponent_team_id" not in df.columns:
        df["opponent_team_id"] = inferred
    else:
        df["opponent_team_id"] = df["opponent_team_id"].where(df["opponent_team_id"].notna() & (df["opponent_team_id"].astype(str).str.len() > 0), inferred)

    # Coerce undecided -> False if pitcher is set (player_id present and not null)
    if "undecided" in df.columns:
        # If undecided missing, initialize False
        undec = df["undecided"]
        # set False when player_id looks valid
        has_pitcher = df["player_id"].notna() & (df["player_id"].astype(str).str.strip() != "")
        # Normalize undecided to boolean
        df["undecided"] = df["undecided"].map(lambda x: str(x).strip().lower() if pd.notna(x) else "")
        df.loc[has_pitcher, "undecided"] = "false"
    else:
        df["undecided"] = "false"

    # Write back
    df.to_csv(PITCHER_PROPS_FIXED, index=False)
    ok("updated opponent_team_id and undecided in pitcher_props_projected_fixed.csv")

def validate_all():
    # Batters projected
    ensure_exists(BATTER_PROJ_FIXED)
    df_bproj = load_csv(BATTER_PROJ_FIXED)
    validate_columns(df_bproj, REQUIRED[BATTER_PROJ_FIXED], BATTER_PROJ_FIXED)

    # Batters expanded
    ensure_exists(BATTER_EXP_FIXED)
    df_bexp = load_csv(BATTER_EXP_FIXED)
    validate_columns(df_bexp, REQUIRED[BATTER_EXP_FIXED], BATTER_EXP_FIXED)

    # Pitcher props (after backfill we’ll re-check)
    ensure_exists(PITCHER_PROPS_FIXED)
    df_pp = load_csv(PITCHER_PROPS_FIXED)
    validate_columns(df_pp, REQUIRED[PITCHER_PROPS_FIXED], PITCHER_PROPS_FIXED)

    # Pitcher mega Z
    ensure_exists(PITCHER_MEGAZ_FIXED)
    df_mz = load_csv(PITCHER_MEGAZ_FIXED)
    validate_columns(df_mz, REQUIRED[PITCHER_MEGAZ_FIXED], PITCHER_MEGAZ_FIXED)

    ok("validated all *_fixed.csv outputs")

def main():
    try:
        # 1) validate presence/columns
        validate_all()
        # 2) backfill pitcher opponent & undecided
        backfill_pitcher_props()
        # 3) re-validate pitcher props (ensures still has required columns)
        df_pp = load_csv(PITCHER_PROPS_FIXED)
        validate_columns(df_pp, REQUIRED[PITCHER_PROPS_FIXED], PITCHER_PROPS_FIXED)
        ok("final validation passed")
    except Exception as e:
        err(str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
