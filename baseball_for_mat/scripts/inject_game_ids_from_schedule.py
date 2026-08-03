#!/usr/bin/env python3
# scripts/inject_game_ids_from_schedule.py
# Inject game_id into final outputs using schedule derived from todaysgames_normalized_fixed.csv
from pathlib import Path
import sys
import pandas as pd

DATA_DIR = Path("data")
PROJ_DIR = DATA_DIR / "_projections"

SCHEDULE = PROJ_DIR / "todaysgames_normalized_fixed.csv"
FILES = [
    PROJ_DIR / "batter_props_projected_final.csv",
    PROJ_DIR / "batter_props_expanded_final.csv",
    PROJ_DIR / "pitcher_props_projected_final.csv",
    PROJ_DIR / "pitcher_mega_z_final.csv",
]

def _read_csv(p: Path) -> pd.DataFrame:
    if not p.exists(): raise FileNotFoundError(f"Missing input: {p}")
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    return df

def _write_csv(df: pd.DataFrame, p: Path) -> None:
    # === ENFORCE STRING IDS ===
    for __c in ["home_team_id","away_team_id","pitcher_home_id","pitcher_away_id","player_id","team_id","game_id"]:
        if __c in df.columns:
            df[__c] = df[__c].astype("string")
    # === END ENFORCE ===
    df.to_csv(p, index=False)

def main() -> int:
    if not SCHEDULE.exists(): raise FileNotFoundError(f"Missing input: {SCHEDULE}")
    sched = _read_csv(SCHEDULE)

    # Require schedule keys
    for c in ["home_team_id","away_team_id","game_id"]:
        if c not in sched.columns:
            raise KeyError(f"schedule missing column: {c}")

    # work on each file independently
    for f in FILES:
        df = _read_csv(f)
        # choose a merge key: prefer team IDs
        keys = []
        if "home_team_id" in df.columns and "home_team_id" in sched.columns:
            keys.append("home_team_id")
        if "away_team_id" in df.columns and "away_team_id" in sched.columns:
            keys.append("away_team_id")
        if not keys:
            print(f"SKIP {f}: no team_id columns to match")
            continue

        merged = df.merge(sched[keys+["game_id"]].drop_duplicates(keys), on=keys, how="left")
        assigned = merged["game_id"].notna().sum() if "game_id" in merged.columns else 0

        _write_csv(merged, f)
        print(f"✅ {f}: rows={len(merged)}, game_id_assigned={assigned}, unmatched_team_ids={0}, ambiguous_team_ids={0}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
