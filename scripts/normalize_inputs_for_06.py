#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

ROOT = Path(".")
DATA = ROOT / "data"

def normalize_csv(path: Path) -> None:
    if not path.exists():
        return
    # Force all columns to string on read
    df = pd.read_csv(path, low_memory=False, dtype=str, keep_default_na=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def main():
    targets = [
        DATA / "_projections" / "todaysgames_normalized_fixed.csv",
        DATA / "Data" / "pitchers.csv",
        DATA / "manual" / "stadium_master.csv",
        DATA / "_projections" / "pitcher_props_projected.csv",
        DATA / "_projections" / "pitcher_mega_z.csv",
        DATA / "_projections" / "batter_props_projected.csv",
        DATA / "_projections" / "batter_props_expanded.csv",
        DATA / "_projections" / "pitcher_props_projected_fixed.csv",
        DATA / "_projections" / "pitcher_mega_z_fixed.csv",
        DATA / "_projections" / "batter_props_projected_fixed.csv",
        DATA / "_projections" / "batter_props_expanded_fixed.csv",
    ]

    for path in targets:
        normalize_csv(path)

    print("normalize_inputs_for_06.py: enforced string dtypes for ALL columns in 06 inputs.")

if __name__ == "__main__":
    main()
