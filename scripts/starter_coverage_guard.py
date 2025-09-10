#!/usr/bin/env python3
# scripts/starter_coverage_guard.py
# Purpose: Fail the run if any today's starters are missing from the
#          enriched pitcher projections. Uses pitcher_props_projected.csv
#          as the coverage source (no new files or paths introduced).

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

# Inputs
STARTERS_IN = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
PROJECTED_IN = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"  # <- use enriched projections

# Outputs (kept the same filenames to avoid downstream changes)
OUT_DIR = ROOT / "summaries" / "projections"
OUT_DIR.mkdir(parents=True, exist_ok=True)
COVERAGE_OUT = OUT_DIR / "mega_z_starter_coverage.csv"
MISSING_OUT = OUT_DIR / "mega_z_starter_missing.csv"

def main() -> int:
    # --- Load today's starters (expected coverage set) ---
    sp = pd.read_csv(STARTERS_IN, dtype=str)
    # Normalize core columns as strings; drop rows with missing player_id
    for c in ("game_id", "team_id", "opponent_team_id", "player_id"):
        if c in sp.columns:
            sp[c] = sp[c].astype(str)
    sp = sp.dropna(subset=["player_id"])
    sp = sp[sp["player_id"].str.strip().ne("")]

    starters_today = sp["player_id"].unique().tolist()

    # --- Load coverage from enriched projections ---
    proj = pd.read_csv(PROJECTED_IN, dtype=str)
    # Be defensive about column names; require player_id
    if "player_id" not in proj.columns:
        raise RuntimeError(f"{PROJECTED_IN} is missing required column: 'player_id'")

    proj["player_id"] = proj["player_id"].astype(str)

    have_ids = set(proj["player_id"].unique().tolist())
    need_ids = set(starters_today)

    missing_ids = sorted(need_ids - have_ids)

    # --- Coverage table (for inspection) ---
    sp_cov = sp.copy()
    sp_cov["in_projected"] = sp_cov["player_id"].isin(have_ids).astype(int)

    # Keep a compact, predictable column order if available
    cols = [c for c in ["player_id", "game_id", "team_id", "opponent_team_id", "in_projected"] if c in sp_cov.columns]
    sp_cov[cols].to_csv(COVERAGE_OUT, index=False)

    # Missing table
    if missing_ids:
        miss_df = sp_cov[sp_cov["player_id"].isin(missing_ids)].drop_duplicates(subset=["player_id"])
        miss_cols = [c for c in ["player_id", "game_id", "team_id", "opponent_team_id"] if c in miss_df.columns]
        miss_df[miss_cols].to_csv(MISSING_OUT, index=False)
    else:
        # Write an empty file with headers for consistency
        pd.DataFrame(columns=["player_id", "game_id", "team_id", "opponent_team_id"]).to_csv(MISSING_OUT, index=False)

    # Maintain prior log/message style (string mentions "pitcher_mega_z" to keep downstream expectations/log parsing stable)
    if missing_ids:
        msg = f"Starter coverage failure: {len(missing_ids)} starter(s) absent in pitcher_mega_z."
        print("Starter coverage failure: {} starter(s) absent in pitcher_mega_z.".format(len(missing_ids)))
        print(f"Wrote {COVERAGE_OUT.relative_to(ROOT)} and {MISSING_OUT.relative_to(ROOT)} with details.")
        raise RuntimeError(msg)
    else:
        print("Starter coverage OK: all starters present in projections.")
        print(f"Wrote {COVERAGE_OUT.relative_to(ROOT)} and {MISSING_OUT.relative_to(ROOT)} with details.")
        return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(1)
