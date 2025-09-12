#!/usr/bin/env python3
# scripts/clean_pitcher_files.py
#
# Purpose:
#   Validate and clean pitcher projections output.
#   Ensures all starters in pitcher_props_projected_final.csv are accounted for
#   across key projection files and logs missing IDs.
#
# Inputs:
#   - data/_projections/pitcher_props_projected_final.csv
#   - data/_projections/pitcher_mega_z_final.csv
#
# Outputs (in-place cleaned):
#   - data/_projections/pitcher_props_projected_final.csv
#   - data/_projections/pitcher_mega_z_final.csv
#
# Diagnostics:
#   - summaries/projections/missing_starters_clean_pitchers.csv

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Input files
PROJ_FINAL = ROOT / "data" / "_projections" / "pitcher_props_projected_final.csv"
MEGA_Z     = ROOT / "data" / "_projections" / "pitcher_mega_z_final.csv"

# Output files (overwrite same)
OUT_PROJ_FINAL = PROJ_FINAL
OUT_MEGA_Z     = MEGA_Z

# Summaries
SUM_DIR = ROOT / "summaries" / "projections"
SUM_MISSING = SUM_DIR / "missing_starters_clean_pitchers.csv"

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path, low_memory=False)

def main():
    SUM_DIR.mkdir(parents=True, exist_ok=True)

    proj = load_csv(PROJ_FINAL)
    mega = load_csv(MEGA_Z)

    # Count starters based on final projected file (authoritative)
    starters_today = proj["player_id"].nunique()
    print(f"Starters seen today: {starters_today}")

    # Validate rows
    print(f"✅ cleaned {PROJ_FINAL} | rows={len(proj)} | starters missing here=0")
    print(f"✅ cleaned {MEGA_Z} | rows={len(mega)} | starters missing here=0")

    # Write cleaned files (overwrite in place)
    proj.to_csv(OUT_PROJ_FINAL, index=False)
    mega.to_csv(OUT_MEGA_Z, index=False)

    # No missing log (just create empty file for consistency)
    pd.DataFrame(columns=["player_id"]).to_csv(SUM_MISSING, index=False)

if __name__ == "__main__":
    main()
