#!/usr/bin/env python3
# scripts/fix_outputs_generate_fixed_files.py
# Normalize/clean published outputs to create "*_fixed.csv" versions used downstream
import sys
import pandas as pd
import numpy as np
from pathlib import Path

# ----- Paths -----
PROJ_DIR = Path("data") / "_projections"
OUT_PITCHER_PROPS_FIXED = PROJ_DIR / "pitcher_props_projected_fixed.csv"
OUT_PITCHER_MEGAZ_FIXED = PROJ_DIR / "pitcher_mega_z_fixed.csv"
OUT_BATTER_PROJ_FIXED   = PROJ_DIR / "batter_props_projected_fixed.csv"
OUT_BATTER_EXP_FIXED    = PROJ_DIR / "batter_props_expanded_fixed.csv"

SRC_PITCHER_PROPS = PROJ_DIR / "pitcher_props_projected_final.csv"
SRC_PITCHER_MEGAZ = PROJ_DIR / "pitcher_mega_z_final.csv"
SRC_BATTER_PROJ   = PROJ_DIR / "batter_props_projected_final.csv"
SRC_BATTER_EXP    = PROJ_DIR / "batter_props_expanded_final.csv"

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df

def _write_fixed(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # === ENFORCE STRING IDS ===
    for __c in ["home_team_id","away_team_id","pitcher_home_id","pitcher_away_id","player_id","team_id","game_id"]:
        if __c in df.columns:
            df[__c] = df[__c].astype("string")
    # === END ENFORCE ===
    df.to_csv(path, index=False)

def main() -> int:
    # Pitcher props
    pp = _read_csv(SRC_PITCHER_PROPS)
    _write_fixed(pp.copy(), OUT_PITCHER_PROPS_FIXED)

    # Pitcher mega z
    pmz = _read_csv(SRC_PITCHER_MEGAZ)
    _write_fixed(pmz.copy(), OUT_PITCHER_MEGAZ_FIXED)

    # Batter projected
    bp = _read_csv(SRC_BATTER_PROJ)
    _write_fixed(bp.copy(), OUT_BATTER_PROJ_FIXED)

    # Batter expanded
    be = _read_csv(SRC_BATTER_EXP)
    _write_fixed(be.copy(), OUT_BATTER_EXP_FIXED)

    print(f"✔ Wrote: {OUT_PITCHER_PROPS_FIXED} rows= {len(pp)}")
    print(f"✔ Wrote: {OUT_PITCHER_MEGAZ_FIXED} rows= {len(pmz)}")
    print(f"✔ Wrote: {OUT_BATTER_PROJ_FIXED} rows= {len(bp)}")
    print(f"✔ Wrote: {OUT_BATTER_EXP_FIXED} rows= {len(be)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
