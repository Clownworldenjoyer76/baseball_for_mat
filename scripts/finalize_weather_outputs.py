#!/usr/bin/env python3
# File: /mnt/data/baseball_project/baseball_for_mat-main/scripts/finalize_weather_outputs.py
"""
File: scripts/finalize_weather_outputs.py
Purpose: Insert/update columns in weather outputs using normalized games.
 - Insert/update `game_id` in data/weather_input.csv
 - Insert/update `home_team`, `away_team` in data/weather_adjustments.csv
Source of truth: data/raw/todaysgames_normalized.csv
Join keys: home_team_id + away_team_id
"""
import sys
import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

WEATHER_INPUT = REPO_ROOT / "data" / "weather_input.csv"
WEATHER_ADJ   = REPO_ROOT / "data" / "weather_adjustments.csv"
TODAYS_GAMES  = REPO_ROOT / "data" / "raw" / "todaysgames_normalized.csv"

def fail(msg: str, code: int = 1):
    print(f"INSUFFICIENT INFORMATION: {msg}")
    sys.exit(code)

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        fail(f"Missing file: {path}")
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"CANNOT COMPLY: Unable to read {path} -> {e}")
        sys.exit(2)

def ensure_cols(df: pd.DataFrame, cols: list, ctx: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        fail(f"{ctx} missing required columns: {missing}")

def main():
    wi = load_csv(WEATHER_INPUT)
    wa = load_csv(WEATHER_ADJ)
    tg = load_csv(TODAYS_GAMES)

    required_keys = ["home_team_id", "away_team_id"]
    ensure_cols(wi, required_keys, "weather_input")
    ensure_cols(wa, required_keys, "weather_adjustments")
    ensure_cols(tg, required_keys + ["game_id", "home_team", "away_team"], "todaysgames_normalized")

    games_map = tg[["home_team_id","away_team_id","game_id","home_team","away_team"]].copy()

    # Update weather_input with game_id
    wi_updated = wi.merge(
        games_map[["home_team_id","away_team_id","game_id"]],
        on=["home_team_id","away_team_id"],
        how="left"
    )
    if "game_id_x" in wi_updated.columns and "game_id_y" in wi_updated.columns:
        wi_updated["game_id"] = wi_updated["game_id_y"].where(
            wi_updated["game_id_y"].notna(), wi_updated["game_id_x"]
        )
        wi_updated = wi_updated.drop(columns=["game_id_x","game_id_y"])

    # Update weather_adjustments with team codes
    wa_updated = wa.merge(
        games_map[["home_team_id","away_team_id","home_team","away_team"]],
        on=["home_team_id","away_team_id"],
        how="left",
        suffixes=("","_from_games")
    )
    for col in ["home_team","away_team"]:
        src = f"{col}_from_games"
        if src in wa_updated.columns:
            wa_updated[col] = wa_updated[src].where(wa_updated[src].notna(), wa_updated.get(col))
            wa_updated = wa_updated.drop(columns=[src])

    wi_updated.to_csv(WEATHER_INPUT, index=False)
    wa_updated.to_csv(WEATHER_ADJ, index=False)

    print(f"UPDATED: {WEATHER_INPUT}")
    print(f"UPDATED: {WEATHER_ADJ}")

if __name__ == "__main__":
    main()
