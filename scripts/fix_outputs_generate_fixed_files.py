#!/usr/bin/env python3
# scripts/fix_outputs_generate_fixed_files.py
#
# Purpose:
# - Produce *_fixed.csv files that are safe for downstream usage.
# - Resolve starters context strictly by player_id (no name matching).
# - Avoid duplicate column collisions on merges.
# - Preserve ALL columns in batter files; do not drop anything.
# - Log diagnostics instead of crashing when something can’t be matched.

import pandas as pd
import numpy as np
from pathlib import Path

# ----- Paths -----
PROJ_DIR = Path("data/_projections")
END_DIR  = Path("data/end_chain/final")
SUM_DIR  = Path("summaries/projections")

PP_FINAL         = PROJ_DIR / "pitcher_props_projected_final.csv"
PMZ_FINAL        = PROJ_DIR / "pitcher_mega_z_final.csv"
BAT_PROJ_FINAL   = PROJ_DIR / "batter_props_projected_final.csv"
BAT_EXP_FINAL    = PROJ_DIR / "batter_props_expanded_final.csv"

PP_FIXED         = PROJ_DIR / "pitcher_props_projected_fixed.csv"
PMZ_FIXED        = PROJ_DIR / "pitcher_mega_z_fixed.csv"
BAT_PROJ_FIXED   = PROJ_DIR / "batter_props_projected_fixed.csv"
BAT_EXP_FIXED    = PROJ_DIR / "batter_props_expanded_fixed.csv"

TODAY_FIXED_CAND = PROJ_DIR / "todaysgames_normalized_fixed.csv"
TODAY_RAW_CAND   = Path("data/raw/todaysgames_normalized.csv")

SUM_DIR.mkdir(parents=True, exist_ok=True)
PROJ_DIR.mkdir(parents=True, exist_ok=True)
END_DIR.mkdir(parents=True, exist_ok=True)

def write_text(path: Path, text: str):
    path.write_text(text, encoding="utf-8")

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)

def main():
    # 1) Load inputs
    # Prefer todaysgames_normalized_fixed if present; otherwise fall back to raw.
    todays_path = TODAY_FIXED_CAND if TODAY_FIXED_CAND.exists() else TODAY_RAW_CAND
    todays = safe_read_csv(todays_path)

    pp = safe_read_csv(PP_FINAL)
    pmz = safe_read_csv(PMZ_FINAL)
    bat_proj = safe_read_csv(BAT_PROJ_FINAL)
    bat_exp  = safe_read_csv(BAT_EXP_FINAL)

    # 2) Build starters (two rows per game: home & away), strictly by IDs
    required_cols = [
        "game_id", "home_team_id", "away_team_id",
        "pitcher_home_id", "pitcher_away_id", "park_factor"
    ]
    missing_req = [c for c in required_cols if c not in todays.columns]
    if missing_req:
        raise RuntimeError(f"todaysgames file missing required columns: {missing_req}")

    # Normalize id columns to numeric where possible
    for c in ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]:
        todays[c] = pd.to_numeric(todays[c], errors="coerce")

    # Create starters rows
    home = todays[["game_id","home_team_id","away_team_id","pitcher_home_id","park_factor"]].copy()
    home.rename(columns={
        "home_team_id":"team_id",
        "away_team_id":"opponent_team_id",
        "pitcher_home_id":"player_id"
    }, inplace=True)
    home["role"] = "HOME"

    away = todays[["game_id","home_team_id","away_team_id","pitcher_away_id","park_factor"]].copy()
    away.rename(columns={
        "away_team_id":"team_id",
        "home_team_id":"opponent_team_id",
        "pitcher_away_id":"player_id"
    }, inplace=True)
    away["role"] = "AWAY"

    starters = pd.concat([home, away], ignore_index=True)
    # Keep only starter rows with a valid player_id
    starters = starters[pd.to_numeric(starters["player_id"], errors="coerce").notna()].copy()
    starters["player_id"] = starters["player_id"].astype(np.int64, errors="ignore")

    # Drop any lingering *_pp columns to avoid collisions on re-runs
    starters = starters.drop(columns=starters.filter(regex=r"_pp$").columns, errors="ignore")

    # 3) Prepare a *stats-only* view from pitcher_props_projected_final.csv
    #    DO NOT bring context columns that collide (city/state/timezone/role/team_id/park_factor/game_id/is_dome).
    #    Keep only stat-ish columns that downstream may need.
    stat_candidates = [
        "player_id",
        "adj_woba_weather", "adj_woba_park", "adj_woba_combined",
        "proj_hits", "proj_hr", "proj_avg", "proj_slg",
        "k_percent_eff", "bb_percent_eff",
        "innings_pitched", "pa", "ab"
    ]
    have_stats = [c for c in stat_candidates if c in pp.columns]
    pp_stats = pp[have_stats].copy()

    # 4) Merge by player_id (left join from starters)
    pp_ctx = starters.merge(pp_stats, on="player_id", how="left", suffixes=("", "_pp"))

    # 5) Diagnostics: any starters not present in pp_stats?
    missing = pp_ctx[pp_ctx["proj_avg"].isna() & pp_ctx["proj_slg"].isna() & pp_ctx["k_percent_eff"].isna()]
    # Some rows can be legitimately NaN if a prospect has no season line; we still log IDs.
    diag_cols = ["game_id", "team_id", "opponent_team_id", "player_id", "role", "park_factor"]
    (missing[diag_cols] if not missing.empty else pd.DataFrame(columns=diag_cols)).to_csv(
        SUM_DIR / "missing_pitcher_ids.csv", index=False
    )

    # 6) Write fixed pitcher props:
    #    We keep the merged starters+stats as the fixed file for today's slate.
    #    This ensures no duplicate context columns and relies purely on IDs.
    #    Columns order: context, then stats.
    ordered_cols = ["player_id","role","team_id","opponent_team_id","game_id","park_factor"] + [c for c in have_stats if c != "player_id"]
    # Make sure we don't duplicate "player_id" in the tail
    ordered_cols = list(dict.fromkeys([c for c in ordered_cols if c in pp_ctx.columns]))
    pp_ctx[ordered_cols].to_csv(PP_FIXED, index=False)

    # 7) Pitcher mega-z: pass-through as-is to *_fixed (no filtering/truncation)
    pmz.to_csv(PMZ_FIXED, index=False)

    # 8) Batter files: copy verbatim to *_fixed (preserve all columns exactly)
    bat_proj.to_csv(BAT_PROJ_FIXED, index=False)
    bat_exp.to_csv(BAT_EXP_FIXED, index=False)

    # 9) Minimal console output for CI logs
    print(f"✔ Wrote: {PP_FIXED} rows= {len(pp_ctx)}")
    print(f"✔ Wrote: {PMZ_FIXED} rows= {len(pmz)}")
    print(f"✔ Wrote: {BAT_PROJ_FIXED} rows= {len(bat_proj)}")
    print(f"✔ Wrote: {BAT_EXP_FIXED} rows= {len(bat_exp)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        write_text(SUM_DIR / "fix_outputs_generate_fixed_files.error.txt", repr(e))
        raise
