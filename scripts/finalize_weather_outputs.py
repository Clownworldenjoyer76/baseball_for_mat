#!/usr/bin/env python3
# scripts/finalize_weather_outputs.py
#
# Purpose:
# - Make weather outputs align with normalized games and enforce one row per game_id
# - Insert/update columns that downstream steps expect
# - Guarantee UNIQUE game_id in data/weather_adjustments.csv (collapse duplicates)
#
# Reads:
#   data/raw/todaysgames_normalized.csv
#   data/weather_input.csv
#   data/weather_adjustments.csv
#
# Writes:
#   data/weather_input.csv              (updated game_id where missing)
#   data/weather_adjustments.csv        (ONE row per game_id, with weather_factor)
#
# Notes:
# - If multiple rows exist for a game_id in weather_adjustments.csv, we consolidate
#   to a single row by choosing the most recent row if a timestamp column exists,
#   otherwise by taking the mean weather_factor.
# - Columns are kept conservative: at minimum ['game_id','weather_factor'].

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

REPO_ROOT          = Path(".")
GAMES_FILE         = REPO_ROOT / "data" / "raw" / "todaysgames_normalized.csv"
WEATHER_INPUT      = REPO_ROOT / "data" / "weather_input.csv"
WEATHER_ADJ        = REPO_ROOT / "data" / "weather_adjustments.csv"

# Minimal columns we must guarantee
REQ_GAMES_COLS     = {"game_id", "home_team_id", "away_team_id"}
REQ_WI_COLS_BASE   = {"home_team_id", "away_team_id"}
REQ_WA_MIN_COLS    = {"weather_factor"}  # game_id will be added/verified

# Helper ---------------------------------------------------------------

def fail(msg: str, code: int = 1):
    print(msg)
    sys.exit(code)

def load_csv(p: Path, dtype=None) -> pd.DataFrame:
    if not p.exists():
        fail(f"Missing file: {p}")
    try:
        return pd.read_csv(p, dtype=dtype)
    except Exception as e:
        fail(f"Unable to read {p}: {e}", 2)

def ensure_cols(df: pd.DataFrame, needed: set[str], ctx: str):
    miss = [c for c in needed if c not in df.columns]
    if miss:
        fail(f"{ctx}: missing columns {miss}")

def normalize_id_series(s: pd.Series) -> pd.Series:
    # Strict but safe: coerce numeric-like to Int64; leave strings as str of int when possible
    out = pd.to_numeric(s, errors="coerce").astype("Int64")
    return out

# Main -----------------------------------------------------------------

def main():
    # 1) Load games and validate baseline columns
    games = load_csv(GAMES_FILE)
    ensure_cols(games, REQ_GAMES_COLS, str(GAMES_FILE))

    # Normalize ids for robust joining
    for col in ["home_team_id", "away_team_id"]:
        games[col] = normalize_id_series(games[col])

    # 2) WEATHER INPUT: ensure game_id can be aligned/injected where missing
    wi = load_csv(WEATHER_INPUT)
    ensure_cols(wi, REQ_WI_COLS_BASE, str(WEATHER_INPUT))

    # Try to add/repair game_id in weather_input by joining to games on team IDs
    if "game_id" not in wi.columns:
        wi["game_id"] = pd.NA

    # Normalize ids in weather_input to align join
    for col in ["home_team_id", "away_team_id"]:
        if col in wi.columns:
            wi[col] = normalize_id_series(wi[col])

    wi = wi.copy()
    # Join to fetch game_id
    tmp = wi.merge(
        games[["home_team_id", "away_team_id", "game_id"]],
        on=["home_team_id", "away_team_id"],
        how="left",
        validate="m:1",
    )
    # Prefer joined game_id where missing/NA
    tmp["game_id"] = tmp["game_id_y"].where(tmp["game_id_x"].isna(), tmp["game_id_x"])
    tmp = tmp.drop(columns=[c for c in tmp.columns if c.endswith(("_x", "_y"))], errors="ignore")

    # Keep columns stable
    wi_cols = list(wi.columns)
    if "game_id" not in wi_cols:
        wi_cols.append("game_id")
    wi_updated = tmp[wi_cols].copy()

    # 3) WEATHER ADJUSTMENTS: enforce unique game_id
    wa = load_csv(WEATHER_ADJ)
    # Ensure at least a factor column exists; if not, create neutral factor
    if "weather_factor" not in wa.columns:
        wa["weather_factor"] = 1.0
    # Try to attach/repair game_id (some pipelines write without it)
    if "game_id" not in wa.columns:
        # If wa carries home/away ids we can map game_id like weather_input
        for col in ["home_team_id", "away_team_id"]:
            if col in wa.columns:
                wa[col] = normalize_id_series(wa[col])
        wa = wa.merge(
            games[["home_team_id", "away_team_id", "game_id"]],
            on=["home_team_id", "away_team_id"],
            how="left",
            validate="m:1",
        )

    # After the above, we must have game_id + weather_factor
    ensure_cols(wa, {"game_id", "weather_factor"}, str(WEATHER_ADJ))

    # Deduplicate to ONE row per game_id:
    # Prefer the most recent row if a timestamp-like column exists; otherwise mean factor.
    dedup_cols = ["game_id", "weather_factor"]
    wa_work = wa.copy()

    # Identify a timestamp-ish column
    ts_candidates = [c for c in wa_work.columns if c.lower() in {"ts","timestamp","updated_at","pulled_at","retrieved_at"}]
    if ts_candidates:
        ts_col = ts_candidates[0]
        # Sort by timestamp descending and take first per game
        wa_work_sort = wa_work.sort_values(by=[ts_col], ascending=False, kind="stable")
        wa_dedup = wa_work_sort.drop_duplicates(subset=["game_id"], keep="first")
        wa_dedup = wa_dedup[dedup_cols]
    else:
        # No timestamp -> average duplicates (safe + deterministic)
        wa_dedup = (
            wa_work.groupby("game_id", as_index=False)["weather_factor"]
            .mean(numeric_only=True)
        )

    # Final schema: exactly game_id + weather_factor
    wa_updated = wa_dedup[["game_id", "weather_factor"]].copy()

    # 4) Write back
    WEATHER_INPUT.parent.mkdir(parents=True, exist_ok=True)
    wi_updated.to_csv(WEATHER_INPUT, index=False)

    WEATHER_ADJ.parent.mkdir(parents=True, exist_ok=True)
    wa_updated.to_csv(WEATHER_ADJ, index=False)

    print(f"UPDATED: {WEATHER_INPUT}  (rows={len(wi_updated)})")
    print(f"UPDATED: {WEATHER_ADJ}  (rows={len(wa_updated)})")
    # Guarantee uniqueness to satisfy downstream 'validate=\"m:1\"'
    assert wa_updated["game_id"].isna().sum() == 0, "weather_adjustments.csv has NA game_id"
    assert wa_updated["game_id"].duplicated().sum() == 0, "weather_adjustments.csv still has duplicate game_id"

if __name__ == "__main__":
    main()
