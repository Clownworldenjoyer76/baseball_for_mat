#!/usr/bin/env python3
# Purpose: Build today's starting pitchers context and also seed pitcher projections input
# Outputs:
#   - data/raw/startingpitchers_with_opp_context.csv
#   - data/_projections/pitcher_props_projected.csv   (new step)

from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

RAW_DIR   = ROOT / "data" / "raw"
PROJ_DIR  = ROOT / "data" / "_projections"
END_DIR   = ROOT / "data" / "end_chain" / "final"

OUT_RAW   = RAW_DIR / "startingpitchers_with_opp_context.csv"
OUT_FINAL = END_DIR / "startingpitchers.csv"
OUT_PROJ  = PROJ_DIR / "pitcher_props_projected.csv"   # new file

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def main() -> int:
    print(f">> START: project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    print(f"[PATH] OUT_RAW={OUT_RAW}")
    print(f"[PATH] OUT_FINAL={OUT_FINAL}")
    print(f"[PATH] OUT_PROJ={OUT_PROJ}")

    # --- your existing logic for building df (starters) goes here ---
    # For now, assume df is the final DataFrame with at least: game_id, team_id, opponent_team_id, player_id
    df = pd.read_csv(RAW_DIR / "startingpitchers_with_opp_context.csv")  # placeholder if upstream logic is external

    # Write raw and final outputs
    write_csv(df, OUT_RAW)
    write_csv(df, OUT_FINAL)

    # --- NEW STEP: write pitcher_props_projected.csv ---
    proj = df[["player_id", "game_id"]].dropna().drop_duplicates().reset_index(drop=True)
    proj["player_id"] = proj["player_id"].astype(str)
    proj["game_id"]   = proj["game_id"].astype(str)

    write_csv(proj, OUT_PROJ)
    print(f"[OK] wrote {len(proj)} rows -> {OUT_PROJ}")

    print(f"[END] project_prep.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(e)
        sys.exit(1)
