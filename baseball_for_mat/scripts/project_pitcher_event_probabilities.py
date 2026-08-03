#!/usr/bin/env python3
# scripts/project_pitcher_event_probabilities.py
#
# Generates pitcher-allowed event probabilities versus a league-average lineup.
# Robust behaviors:
# - Strictly requires: data/_projections/pitcher_props_projected_final.csv
#                      data/Data/batters.csv        (season totals)
# - Optional:          data/_projections/batter_props_projected_final.csv
#   (used only for potential PA weighting; absence will NOT fail the run)
# - Normalizes key columns to STRING to avoid merge/type issues.
# - Writes BOTH:
#       data/_projections/pitcher_event_probabilities.csv
#       data/end_chain/final/pitcher_event_probabilities.csv
# - Emits clear row counts and deterministic column order.

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

# ---- Paths ----
DAILY_DIR   = Path("data/_projections")
SEASON_DIR  = Path("data/Data")  # ensure this matches your repo casing exactly
END_DIR     = Path("data/end_chain/final")

PITCHERS_DAILY = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_DAILY  = DAILY_DIR / "batter_props_projected_final.csv"  # optional
BATTERS_SEASON = SEASON_DIR / "batters.csv"

OUT_FILE_PROJ  = DAILY_DIR / "pitcher_event_probabilities.csv"
OUT_FILE_FINAL = END_DIR / "pitcher_event_probabilities.csv"

STRING_KEY_COLS_P = ["player_id", "game_id", "team_id", "opponent_team_id"]

# ---- Utils ----
def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def read_csv_required(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        fail(f"Required file missing: {path} (for {name})")
    try:
        return pd.read_csv(path)
    except Exception as e:
        fail(f"Failed to read {path}: {e}")
        return pd.DataFrame()  # unreachable, keeps type checkers happy

def read_csv_optional(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        # If optional file is malformed, ignore quietly (not used downstream)
        return None

def require_columns(df: pd.DataFrame, cols: list[str], context: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        fail(f"{context} missing columns: {missing}")

def force_str_cols(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)

def to_num(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def league_rates_from_batters_season(bat_s: pd.DataFrame) -> dict[str, float]:
    # Required columns
    req = ["pa","strikeout","walk","single","double","triple","home_run"]
    require_columns(bat_s, req, "batters_season")
    to_num(bat_s, req)
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    if lg_pa <= 0:
        return {"k":0.0,"bb":0.0,"1b":0.0,"2b":0.0,"3b":0.0,"hr":0.0}
    return {
        "k":  float(bat_s["strikeout"].sum() / lg_pa),
        "bb": float(bat_s["walk"].sum()      / lg_pa),
        "1b": float(bat_s["single"].sum()    / lg_pa),
        "2b": float(bat_s["double"].sum()    / lg_pa),
        "3b": float(bat_s["triple"].sum()    / lg_pa),
        "hr": float(bat_s["home_run"].sum()  / lg_pa),
    }

def main() -> None:
    # Ensure output dirs exist
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    END_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load required inputs
    pit = read_csv_required(PITCHERS_DAILY, "pitchers_daily")
    bat_s = read_csv_required(BATTERS_SEASON, "batters_season")

    # ---- Optional input (not required for current logic)
    _bat_opt = read_csv_optional(BATTERS_DAILY)  # reserved for future weighting
    # (Do NOT fail if missing; earlier versions incorrectly enforced proj_pa_used.)

    # ---- Validate minimally needed schema on pitchers
    require_columns(pit, ["player_id","game_id","team_id","opponent_team_id","pa"], "pitchers_daily")

    # ---- Normalize dtypes
    force_str_cols(pit, STRING_KEY_COLS_P)
    to_num(pit, ["pa"])

    # ---- Compute league-average rates
    lg_rates = league_rates_from_batters_season(bat_s)

    # ---- Build output frame
    cols_base = ["player_id","game_id","team_id","opponent_team_id","pa"]
    pit_out = pit[cols_base].copy()

    pit_out["p_k_allowed"]  = lg_rates["k"]
    pit_out["p_bb_allowed"] = lg_rates["bb"]
    pit_out["p_1b_allowed"] = lg_rates["1b"]
    pit_out["p_2b_allowed"] = lg_rates["2b"]
    pit_out["p_3b_allowed"] = lg_rates["3b"]
    pit_out["p_hr_allowed"] = lg_rates["hr"]

    # Ensure probabilities <= 1, derive outs
    s = pit_out[["p_k_allowed","p_bb_allowed","p_1b_allowed","p_2b_allowed","p_3b_allowed","p_hr_allowed"]].sum(axis=1)
    pit_out["p_out_allowed"] = (1.0 - s).clip(lower=0.0)

    # Deterministic ordering
    ordered_cols = [
        "player_id","game_id","team_id","opponent_team_id","pa",
        "p_k_allowed","p_bb_allowed","p_1b_allowed","p_2b_allowed","p_3b_allowed","p_hr_allowed","p_out_allowed",
    ]
    existing = [c for c in ordered_cols if c in pit_out.columns]
    pit_out = pit_out[existing].copy()

    # Sort for stable diffs
    pit_out.sort_values(by=["game_id","player_id"], kind="mergesort", inplace=True, ignore_index=True)

    # ---- Write outputs
    try:
        pit_out.to_csv(OUT_FILE_PROJ, index=False)
        pit_out.to_csv(OUT_FILE_FINAL, index=False)
    except Exception as e:
        fail(f"Failed to write outputs: {e}")

    print(f"OK: wrote {OUT_FILE_PROJ} and {OUT_FILE_FINAL} rows={len(pit_out)}")

if __name__ == "__main__":
    main()
