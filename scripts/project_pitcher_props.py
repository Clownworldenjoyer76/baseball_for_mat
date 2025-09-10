#!/usr/bin/env python3
# Purpose: produce data/_projections/pitcher_props_projected.csv
# Ensures the output includes player_id + game_id + team_id + opponent_team_id
# by enriching with startingpitchers_with_opp_context.csv (long form).
# No new paths.

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path

VERSION = "v3-enriched"

ROOT = Path(__file__).resolve().parents[1]
PROJ_DIR = ROOT / "data" / "_projections"
RAW_DIR = ROOT / "data" / "raw"

IN_PROPS_CORE = PROJ_DIR / "pitcher_props_projected_core.csv"      # if your pipeline produces a core file; else adjust below
OUT_PROPS = PROJ_DIR / "pitcher_props_projected.csv"
SP_WITH_OPP = RAW_DIR / "startingpitchers_with_opp_context.csv"

REQ_FROM_SP = ["player_id", "game_id", "team_id", "opponent_team_id"]

def log(msg: str) -> None:
    print(msg, flush=True)

def _as_str(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)

def main() -> int:
    log(f"[project_pitcher_props] VERSION={VERSION} @ {Path(__file__).resolve()}")

    # Your pipeline previously wrote directly to pitcher_props_projected.csv (source=enriched).
    # If you already have that build in memory earlier in this script, use it.
    # Otherwise, read the existing enriched file if present, or a core file you build upstream.
    df_proj = None
    if OUT_PROPS.exists():
        df_proj = pd.read_csv(OUT_PROPS, dtype=str)
    elif IN_PROPS_CORE.exists():
        df_proj = pd.read_csv(IN_PROPS_CORE, dtype=str)
    else:
        raise RuntimeError("Missing input for pitcher projections. Expected existing enriched "
                           f"{OUT_PROPS} or core {IN_PROPS_CORE}.")

    # Ensure player_id exists; everything else we can enrich.
    if "player_id" not in df_proj.columns:
        raise RuntimeError("Missing required column in projections: 'player_id'")

    # Enrich with game/team context from startingpitchers_with_opp_context (long 4-col file)
    if not SP_WITH_OPP.exists():
        raise RuntimeError(f"Missing {SP_WITH_OPP} to attach game/team context.")

    sp = pd.read_csv(SP_WITH_OPP, dtype=str)
    missing_sp = [c for c in REQ_FROM_SP if c not in sp.columns]
    if missing_sp:
        raise RuntimeError(f"{SP_WITH_OPP} missing required columns: {missing_sp}")

    # Merge on player_id; keep multiplicity if the player appears twice (DH/unknown)—downstream cleans.
    # If df_proj already has these columns, don't lose them—only fill missing.
    enrich = sp[REQ_FROM_SP].drop_duplicates()

    # Left merge by player_id
    merged = df_proj.merge(enrich, on="player_id", how="left", suffixes=("", "_sp"))
    # If any of the ids exist on df_proj, prefer them; otherwise take the _sp value.
    for c in ["game_id", "team_id", "opponent_team_id"]:
        if c not in merged.columns:
            merged[c] = merged[f"{c}_sp"]
        else:
            merged[c] = merged[c].fillna(merged[f"{c}_sp"])
        if f"{c}_sp" in merged.columns:
            merged.drop(columns=[f"{c}_sp"], inplace=True)

    # Final checks
    need = ["player_id", "game_id", "team_id", "opponent_team_id"]
    missing = [c for c in need if c not in merged.columns]
    if missing:
        raise RuntimeError(f"Missing required column(s): {missing}")

    # Normalize as strings; fill UNKNOWN where still missing
    for c in need:
        merged[c] = merged[c].astype(str).fillna("UNKNOWN").replace({"nan": "UNKNOWN"})

    PROJ_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_PROPS, index=False)
    log(f"Wrote: {OUT_PROPS} (rows={len(merged)})  source=enriched")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(1)
