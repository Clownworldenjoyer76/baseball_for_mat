#!/usr/bin/env python3
# scripts/finalize_weather_outputs.py
#
# Guarantees ONE weather row per game_id for downstream merges.
# Strategy:
#   1) Load today's games -> game_id, home_team_id, away_team_id
#   2) Load weather_adjustments (12 venue rows typical)
#   3) Map to game_id using the best available keys:
#        a) If weather_adjustments already has game_id -> use it
#        b) Else if it has home_team_id + away_team_id -> join to games (m:1)
#        c) Else if weather_input has those IDs -> map via weather_input first
#   4) Collapse to one row per game_id (prefer latest by timestamp-like column,
#      else mean of weather_factor).
#   5) Assert unique game_id count == number of games today; hard-fail if not.

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(".")
GAMES = ROOT / "data" / "raw" / "todaysgames_normalized.csv"
WEATHER_INPUT = ROOT / "data" / "weather_input.csv"
WEATHER_ADJ = ROOT / "data" / "weather_adjustments.csv"

def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}")
    sys.exit(code)

def load_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        fail(f"Missing file: {p}")
    try:
        return pd.read_csv(p)
    except Exception as e:
        fail(f"Unable to read {p}: {e}", 2)

def to_int64(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def attach_game_id(wa: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    wa = wa.copy()
    if "game_id" in wa.columns and wa["game_id"].notna().any():
        # Normalize type and return
        wa["game_id"] = to_int64(wa["game_id"])
        return wa

    # Try join via home/away ids
    key_cols = ["home_team_id", "away_team_id"]
    have_keys = all(c in wa.columns for c in key_cols)
    if have_keys:
        for c in key_cols:
            wa[c] = to_int64(wa[c])
        merged = wa.merge(
            games[["game_id", "home_team_id", "away_team_id"]],
            on=key_cols, how="left", validate="m:1"
        )
        return merged

    # As a last resort, try mapping from weather_input if it has the ids
    wi = load_csv(WEATHER_INPUT)
    if all(c in wi.columns for c in key_cols):
        for c in key_cols:
            wi[c] = to_int64(wi[c])
        wi = wi.merge(
            games[["game_id", "home_team_id", "away_team_id"]],
            on=key_cols, how="left", validate="m:1"
        )[key_cols + ["game_id"]].drop_duplicates()
        # best-effort merge back
        merged = wa.merge(wi, on=key_cols, how="left", validate="m:1")
        return merged

    fail("Could not determine game_id for weather rows (no suitable keys).")

def collapse_one_row_per_game(wa: pd.DataFrame) -> pd.DataFrame:
    if "weather_factor" not in wa.columns:
        # neutral factor if upstream didn’t compute it (shouldn’t happen)
        wa = wa.copy()
        wa["weather_factor"] = 1.0

    # Prefer latest by timestamp-like column if present
    ts_candidates = [c for c in wa.columns if c.lower() in {
        "ts","timestamp","updated_at","pulled_at","retrieved_at","asof"
    }]
    if ts_candidates:
        ts = ts_candidates[0]
        wa_sorted = wa.sort_values(by=[ts], ascending=False, kind="stable")
        dedup = wa_sorted.dropna(subset=["game_id"]).drop_duplicates(subset=["game_id"], keep="first")
        return dedup[["game_id","weather_factor"]]

    # Otherwise mean per game
    dedup = (
        wa.dropna(subset=["game_id"])
          .groupby("game_id", as_index=False)["weather_factor"]
          .mean(numeric_only=True)
    )
    return dedup[["game_id","weather_factor"]]

def main():
    games = load_csv(GAMES)
    for c in ["game_id","home_team_id","away_team_id"]:
        if c not in games.columns:
            fail(f"{GAMES} missing required column: {c}")
    games["game_id"] = to_int64(games["game_id"])
    games["home_team_id"] = to_int64(games["home_team_id"])
    games["away_team_id"] = to_int64(games["away_team_id"])

    wa_raw = load_csv(WEATHER_ADJ)
    wa_with_ids = attach_game_id(wa_raw, games)
    wa_with_ids["game_id"] = to_int64(wa_with_ids["game_id"])

    wa_final = collapse_one_row_per_game(wa_with_ids).copy()
    wa_final["game_id"] = to_int64(wa_final["game_id"])

    # Hard correctness check
    games_today = games["game_id"].nunique()
    weather_rows = wa_final["game_id"].nunique()

    print(f"Games today: {games_today}  |  Weather rows (unique game_id): {weather_rows}")

    if weather_rows != games_today:
        # Write anyway for debugging, but fail to prevent silent bad merges
        WEATHER_ADJ.parent.mkdir(parents=True, exist_ok=True)
        wa_final.to_csv(WEATHER_ADJ, index=False)
        fail(f"Weather rows != games today ({weather_rows} != {games_today}). Upstream mapping incomplete.", 3)

    WEATHER_ADJ.parent.mkdir(parents=True, exist_ok=True)
    wa_final.to_csv(WEATHER_ADJ, index=False)
    print(f"WROTE {WEATHER_ADJ} with {len(wa_final)} rows (unique per game_id)")

if __name__ == "__main__":
    main()
