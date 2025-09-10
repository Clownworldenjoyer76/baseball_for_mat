#!/usr/bin/env python3
# scripts/project_prep.py
# Purpose: build startingpitchers.csv and startingpitchers_with_opp_context.csv
# Works with long-format startingpitchers.csv (one pitcher per row).
# No new files or paths are introduced.

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---- Paths (unchanged destinations) ----
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
END_DIR = ROOT / "data" / "end_chain" / "final"

STARTING_PITCHERS_OUT = END_DIR / "startingpitchers.csv"
WITH_OPP_OUT = RAW_DIR / "startingpitchers_with_opp_context.csv"

VERSION = "v3-longfmt"

def log(msg: str) -> None:
    print(msg, flush=True)

def _require_cols(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing required columns: {missing}")

def main() -> int:
    log(f">> START: project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_prep] VERSION={VERSION} @ {Path(__file__).resolve()}")

    # -------------------------------------------------------------------------
    # INPUT: read existing long-format startingpitchers.csv
    # -------------------------------------------------------------------------
    if not STARTING_PITCHERS_OUT.exists():
        raise RuntimeError(f"{STARTING_PITCHERS_OUT} not found. Upstream must create it before this step.")

    sp = pd.read_csv(STARTING_PITCHERS_OUT, dtype=str)

    # Must have at least these columns in long format
    base_needed = ["game_id", "team_id", "player_id"]
    _require_cols(sp, base_needed, "startingpitchers.csv")

    # Normalize dtypes (defensive)
    for c in base_needed:
        sp[c] = sp[c].astype(str)

    # -------------------------------------------------------------------------
    # OUTPUT 1: rewrite startingpitchers.csv exactly as-is (idempotent)
    # -------------------------------------------------------------------------
    STARTING_PITCHERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    sp.to_csv(STARTING_PITCHERS_OUT, index=False)
    log(f"project_prep: wrote {STARTING_PITCHERS_OUT.relative_to(ROOT)} (rows={len(sp)})")

    # -------------------------------------------------------------------------
    # OUTPUT 2: build startingpitchers_with_opp_context.csv
    # Required columns for downstream: game_id, team_id, opponent_team_id, player_id
    #
    # Derive opponent_team_id from other row in the same game_id.
    # We do NOT introduce any new inputs. We rely solely on sp.
    # -------------------------------------------------------------------------
    # Build opponent map via self-merge on game_id, exclude same team_id
    left = sp[["game_id", "team_id"]].copy()
    right = sp[["game_id", "team_id"]].rename(columns={"team_id": "opponent_team_id"})
    opp = (
        left.merge(right, on="game_id", how="inner")
            .query("team_id != opponent_team_id")
            .drop_duplicates(subset=["game_id", "team_id"])  # one opponent per (game, team)
    )

    # Merge opponent back to get player_id
    with_opp = (
        sp[["game_id", "team_id", "player_id"]]
        .merge(opp, on=["game_id", "team_id"], how="left")
    )

    # Defensive: ensure strings and final column order
    for c in ["game_id", "team_id", "opponent_team_id", "player_id"]:
        if c in with_opp.columns:
            with_opp[c] = with_opp[c].astype(str)
    with_opp = with_opp[["game_id", "team_id", "opponent_team_id", "player_id"]]

    # Sanity: warn (via log) if any opponent missing (should be rare, e.g., single-team rows)
    missing_opp = with_opp["opponent_team_id"].isna().sum()
    if missing_opp:
        log(f"[WARN] {missing_opp} row(s) missing opponent_team_id (games without a paired opponent row).")

    WITH_OPP_OUT.parent.mkdir(parents=True, exist_ok=True)
    with_opp.to_csv(WITH_OPP_OUT, index=False)
    log(f"project_prep: wrote {WITH_OPP_OUT.relative_to(ROOT)} (rows={len(with_opp)})")

    log(f"[END] project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(1)
