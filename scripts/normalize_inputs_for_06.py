#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

ROOT = Path(".")
DATA = ROOT / "data"

# Helper: cast selected columns to Python str (not pandas 'string[pyarrow]')
def cast_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def normalize_csv(path: Path, id_cols: list[str]) -> None:
    if not path.exists():
        return
    df = pd.read_csv(path, low_memory=False)
    df = cast_str(df, id_cols)
    # Write back without index, preserving everything else
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def main():
    # Map of files -> the ID columns to force to string if present
    targets: dict[Path, list[str]] = {
        # Upstream inputs
        DATA / "_projections" / "todaysgames_normalized_fixed.csv": [
            "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id", "game_id"
        ],
        DATA / "Data" / "pitchers.csv": [
            "player_id", "team_id"
        ],
        DATA / "manual" / "stadium_master.csv": [
            "team_id"
        ],

        # Projections that get re-read later in the chain
        DATA / "_projections" / "pitcher_props_projected.csv": ["player_id", "team_id", "game_id"],
        DATA / "_projections" / "pitcher_mega_z.csv":          ["player_id", "team_id", "game_id"],
        DATA / "_projections" / "batter_props_projected.csv":  ["player_id", "team_id", "game_id"],
        DATA / "_projections" / "batter_props_expanded.csv":   ["player_id", "team_id", "game_id"],

        # “fixed” variants if any script re-reads them
        DATA / "_projections" / "pitcher_props_projected_fixed.csv": ["player_id", "team_id", "game_id"],
        DATA / "_projections" / "pitcher_mega_z_fixed.csv":          ["player_id", "team_id", "game_id"],
        DATA / "_projections" / "batter_props_projected_fixed.csv":  ["player_id", "team_id", "game_id"],
        DATA / "_projections" / "batter_props_expanded_fixed.csv":   ["player_id", "team_id", "game_id"],
    }

    for path, cols in targets.items():
        normalize_csv(path, cols)

    print("normalize_inputs_for_06.py: enforced string dtypes for ID columns in 06 inputs.")

if __name__ == "__main__":
    main()
