#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

DATA = Path("data")
PROJ = DATA / "_projections"

ID_COLS = {
    "player_id", "team_id", "home_team_id", "away_team_id",
    "pitcher_home_id", "pitcher_away_id", "game_id"
}

def _fix(src: Path, dst: Path):
    if not src.exists():
        return None
    # Force all columns to string on read to avoid numeric coercion
    df = pd.read_csv(src, low_memory=False, dtype=str, keep_default_na=False)

    # Re-assert string on ID columns explicitly (belt and suspenders)
    for c in df.columns:
        if c in ID_COLS:
            df[c] = df[c].astype(str)

    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False)
    return len(df)

def main():
    outputs = [
        ("pitcher_props_projected.csv", "pitcher_props_projected_fixed.csv"),
        ("pitcher_mega_z.csv",          "pitcher_mega_z_fixed.csv"),
        ("batter_props_projected.csv",  "batter_props_projected_fixed.csv"),
        ("batter_props_expanded.csv",   "batter_props_expanded_fixed.csv"),
        ("todaysgames_normalized.csv",  "todaysgames_normalized_fixed.csv"),  # if present
    ]
    for src_name, dst_name in outputs:
        src = PROJ / src_name
        dst = PROJ / dst_name
        n = _fix(src, dst)
        if n is not None:
            print(f"✔ Wrote: {dst} rows={n}")

if __name__ == "__main__":
    main()
