#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

DATA = Path("data")
PROJ = DATA / "_projections"
RAW  = DATA / "raw"
MAN  = DATA / "manual"

# columns we force to string if present
ID_COLS = {
    # schedule / games
    "todaysgames_normalized_fixed.csv": [
        "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id",
        "home_team", "away_team",  # sometimes appear as numeric
        "game_id"
    ],
    # starters dump
    (RAW / "startingpitchers_with_opp_context.csv"): [
        "player_id", "team_id", "home_team_id", "game_id"
    ],
    # projections we depend on
    "pitcher_props_projected.csv": ["player_id", "team_id", "game_id"],
    "pitcher_props_projected_final.csv": ["player_id", "team_id", "game_id"],
    "batter_props_projected.csv": ["player_id", "team_id", "game_id"],
    "batter_props_projected_final.csv": ["player_id", "team_id", "game_id"],
    "batter_props_expanded.csv": ["player_id", "team_id", "game_id"],
    "batter_props_expanded_final.csv": ["player_id", "team_id", "game_id"],
    # mega-z
    "pitcher_mega_z.csv": ["player_id", "mlb_id", "team_id"],
    "pitcher_mega_z_final.csv": ["player_id", "mlb_id", "team_id"],
    # static masters
    (MAN / "stadium_master.csv"): ["team_id", "home_team_id"],
}

CANDIDATES = [
    PROJ / "todaysgames_normalized_fixed.csv",
    RAW / "startingpitchers_with_opp_context.csv",
    PROJ / "pitcher_props_projected.csv",
    PROJ / "pitcher_props_projected_final.csv",
    PROJ / "batter_props_projected.csv",
    PROJ / "batter_props_projected_final.csv",
    PROJ / "batter_props_expanded.csv",
    PROJ / "batter_props_expanded_final.csv",
    PROJ / "pitcher_mega_z.csv",
    PROJ / "pitcher_mega_z_final.csv",
    MAN / "stadium_master.csv",
]

def to_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def normalize_file(path: Path, cols: list[str]) -> tuple[int, int]:
    if not path.exists():
        return (0, 0)
    df = pd.read_csv(path)
    changed = 0
    total = 0
    for c in cols:
        if c in df.columns:
            total += 1
            before = df[c].dtype
            df[c] = to_str(df[c])
            after = df[c].dtype
            if after != before:
                changed += 1
    df.to_csv(path, index=False)
    return (total, changed)

def main():
    print("=== normalize_inputs_for_06 ===")
    for p in CANDIDATES:
        key = p if isinstance(p, Path) else Path(p)
        cols = ID_COLS.get(key.name, ID_COLS.get(key, []))
        if not cols:
            continue
        total, changed = normalize_file(key, cols)
        if total:
            print(f"✔ {key}  | coerced {changed}/{total} id columns to string")
    print("=== done ===")

if __name__ == "__main__":
    main()
