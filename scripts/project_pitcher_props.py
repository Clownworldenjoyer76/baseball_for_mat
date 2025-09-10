#!/usr/bin/env python3
# scripts/project_pitcher_props.py
# Purpose: build pitcher_props_projected.csv with team_id/opponent_team_id merged from today's starters

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = ROOT / "data" / "cleaned"
RAW_DIR = ROOT / "data" / "raw"
PROJ_DIR = ROOT / "data" / "_projections"

PITCHERS_IN = CLEAN_DIR / "pitchers_normalized_cleaned.csv"
STARTERS_WITH_OPP_IN = RAW_DIR / "startingpitchers_with_opp_context.csv"
PITCHERS_OUT = PROJ_DIR / "pitcher_props_projected.csv"

VERSION = "v3-enriched"

def log(msg: str) -> None:
    print(msg, flush=True)

def read_csv(path: Path, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, **kwargs)

def ensure_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required column(s): {missing}")

def main() -> int:
    log(f">> START: project_pitcher_props.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_pitcher_props] VERSION={VERSION} @ {Path(__file__).resolve()}")

    # ---- Inputs
    if not PITCHERS_IN.exists():
        raise RuntimeError(f"Input not found: {PITCHERS_IN}")
    if not STARTERS_WITH_OPP_IN.exists():
        raise RuntimeError(f"Input not found: {STARTERS_WITH_OPP_IN}")

    pitchers = read_csv(PITCHERS_IN)
    starters = read_csv(STARTERS_WITH_OPP_IN)

    # Required ids
    ensure_columns(pitchers, ["player_id"])
    ensure_columns(starters, ["game_id", "team_id", "opponent_team_id", "player_id"])

    # Filter to today's starters by player_id intersection (keeps schema as-is)
    starter_ids = starters["player_id"].dropna().unique().tolist()
    df = pitchers[pitchers["player_id"].isin(starter_ids)].copy()

    # If pitchers file already has partial ids, keep them as strings
    for c in ["player_id"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # Merge game/team/opponent from starters context
    df = df.merge(
        starters[["game_id", "player_id", "team_id", "opponent_team_id"]],
        on="player_id",
        how="left",
        validate="m:1"
    )

    # Final required columns
    ensure_columns(df, ["player_id", "game_id", "team_id", "opponent_team_id"])

    # Normalize dtypes + no NaN strings in id columns
    for c in ["player_id", "game_id", "team_id", "opponent_team_id"]:
        df[c] = df[c].astype(str).fillna("")

    # Output
    PITCHERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PITCHERS_OUT, index=False)
    log(f"Wrote: {PITCHERS_OUT.as_posix()} (rows={len(df)})  source=enriched")
    log(f"[END] project_pitcher_props.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(1)
