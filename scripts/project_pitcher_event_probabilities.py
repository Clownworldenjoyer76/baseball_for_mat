#!/usr/bin/env python3
# scripts/project_pitcher_event_probabilities.py
#
# Robust version:
# - Normalizes key columns (team_id, opponent_team_id, game_id, player_id) to **string**
#   right after reading inputs so merges never fail on dtype mismatches.
# - Computes pitcher-allowed event probabilities against an average lineup (league rates).
# - Writes both _projections and end_chain/final outputs.
# - Emits clear row counts.

import pandas as pd
from pathlib import Path

DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
OUT_DIR = DAILY_DIR
END_DIR = Path("data/end_chain/final")
OUT_DIR.mkdir(parents=True, exist_ok=True)
END_DIR.mkdir(parents=True, exist_ok=True)

PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"  # used only for PA weights if desired later
BATTERS_SEASON  = SEASON_DIR / "batters.csv"

OUT_FILE_PROJ   = OUT_DIR / "pitcher_event_probabilities.csv"
OUT_FILE_FINAL  = END_DIR / "pitcher_event_probabilities.csv"

STRING_KEY_COLS = ["player_id", "game_id", "team_id", "opponent_team_id"]

def require(df: pd.DataFrame, cols: list[str], name: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing columns: {miss}")

def str_cols(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)

def to_num(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def safe_rate(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce").replace(0, pd.NA)
    out = (n / d)
    return out.fillna(0.0).clip(lower=0.0)

def main():
    # Load
    pit = pd.read_csv(PITCHERS_DAILY)
    bat = pd.read_csv(BATTERS_DAILY)
    bat_s = pd.read_csv(BATTERS_SEASON)

    # Minimal schema requirements
    require(pit, ["player_id","game_id","team_id","opponent_team_id","pa"], str(PITCHERS_DAILY))
    require(bat, ["player_id","team_id","game_id","proj_pa_used"],            str(BATTERS_DAILY))
    require(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(BATTERS_SEASON))

    # --- Normalize dtypes on KEYS to strings to avoid "int64 vs object" merge errors
    str_cols(pit, STRING_KEY_COLS)
    str_cols(bat, ["player_id","team_id","game_id"])  # batter side
    # Convert numeric columns only AFTER keys are harmonized
    to_num(pit, ["pa"])
    to_num(bat, ["proj_pa_used"])
    to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])

    # --- League-average batter event rates (what a pitcher faces vs average lineup)
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    if lg_pa <= 0:
        # Degenerate edge case; keep zeros
        lg_rates = {"k":0.0,"bb":0.0,"1b":0.0,"2b":0.0,"3b":0.0,"hr":0.0}
    else:
        lg_rates = {
            "k":  float(bat_s["strikeout"].sum() / lg_pa),
            "bb": float(bat_s["walk"].sum()      / lg_pa),
            "1b": float(bat_s["single"].sum()    / lg_pa),
            "2b": float(bat_s["double"].sum()    / lg_pa),
            "3b": float(bat_s["triple"].sum()    / lg_pa),
            "hr": float(bat_s["home_run"].sum()  / lg_pa),
        }

    # --- Output skeleton from pitcher daily
    pit_out = pit[["player_id","game_id","team_id","opponent_team_id","pa"]].copy()

    # Use league-average per-PA allowed probabilities (neutral opponent quality for now)
    pit_out["p_k_allowed"]  = lg_rates["k"]
    pit_out["p_bb_allowed"] = lg_rates["bb"]
    pit_out["p_1b_allowed"] = lg_rates["1b"]
    pit_out["p_2b_allowed"] = lg_rates["2b"]
    pit_out["p_3b_allowed"] = lg_rates["3b"]
    pit_out["p_hr_allowed"] = lg_rates["hr"]

    # Ensure probabilities sum <= 1 and derive OUT
    s = pit_out[["p_k_allowed","p_bb_allowed","p_1b_allowed",
                 "p_2b_allowed","p_3b_allowed","p_hr_allowed"]].sum(axis=1)
    pit_out["p_out_allowed"] = (1.0 - s).clip(lower=0.0)

    # Persist
    pit_out.to_csv(OUT_FILE_PROJ, index=False)
    pit_out.to_csv(OUT_FILE_FINAL, index=False)
    print(f"OK: wrote {OUT_FILE_PROJ} and {OUT_FILE_FINAL} rows={len(pit_out)}")

if __name__ == "__main__":
    main()
