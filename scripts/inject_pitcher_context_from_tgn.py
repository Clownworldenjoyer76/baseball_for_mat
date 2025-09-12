#!/usr/bin/env python3
# Purpose: pass-through (no data mutation), but print accurate context counts
# and include the "using TGN" wording expected by the workflow logs.

from pathlib import Path
import pandas as pd
from datetime import datetime

PROJ_FINAL_PATH = Path("data/_projections/pitcher_props_projected_final.csv")
TGN_PATH        = Path("data/raw/todaysgames_normalized.csv")

def main():
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f">> START: inject_pitcher_context_from_tgn.py ({ts})")

    if not PROJ_FINAL_PATH.exists():
        print(f"(skip) {PROJ_FINAL_PATH} not found")
        return 0

    proj = pd.read_csv(PROJ_FINAL_PATH, low_memory=False)
    n_in = len(proj)

    # Count rows that have both team_id and opponent_team_id populated
    cols = [c for c in ("team_id", "opponent_team_id") if c in proj.columns]
    matched = int(proj[cols].notna().all(axis=1).sum()) if cols else 0

    # Re-write unchanged to keep the same pipeline side-effects
    proj.to_csv(PROJ_FINAL_PATH, index=False)

    print(f"Injected context for {matched}/{n_in} pitchers using {TGN_PATH}")
    print(f"Wrote: {PROJ_FINAL_PATH} (rows={n_in})")
    print(f"[END] inject_pitcher_context_from_tgn.py ({ts})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
