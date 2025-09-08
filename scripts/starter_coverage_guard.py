#!/usr/bin/env python3
"""
Starter coverage guard:
- Verifies every starter in today's games appears in pitcher_mega_z output.
- Purely a presence/ID check. No imputation, no backfilling, no league averages.
- Writes diagnostics and fails loudly if any starter is missing.

Outputs:
  summaries/projections/mega_z_starter_coverage.csv
  summaries/projections/mega_z_starter_missing.csv  (only when there are misses)
"""

import sys
from pathlib import Path
import pandas as pd

SUM_DIR = Path("summaries/projections")
SUM_DIR.mkdir(parents=True, exist_ok=True)

def read_first_existing(paths):
    for p in paths:
        fp = Path(p)
        if fp.exists():
            return fp
    raise FileNotFoundError(f"None of the candidate files exist: {paths}")

def to_int_series(s):
    # Robust cast to nullable integer, handling floats/strings/NaN
    return pd.to_numeric(s, errors="coerce").dropna().astype("Int64")

def main():
    # 1) Load today's games (prefer the fixed file, fallback to raw)
    todays_games_path = read_first_existing([
        "data/_projections/todaysgames_normalized_fixed.csv",
        "data/raw/todaysgames_normalized.csv",
        "data/_projections/todaysgames_normalized.csv",
    ])
    tg = pd.read_csv(todays_games_path)

    # Required columns in today's games
    need_tg = {"game_id", "pitcher_home_id", "pitcher_away_id"}
    miss_tg = need_tg - set(tg.columns)
    if miss_tg:
        raise RuntimeError(f"{todays_games_path} missing columns: {sorted(miss_tg)}")

    # Starter IDs from both sides (by player_id only)
    home_ids = to_int_series(tg["pitcher_home_id"])
    away_ids = to_int_series(tg["pitcher_away_id"])
    starters_today = pd.Series(pd.unique(pd.concat([home_ids, away_ids], ignore_index=True))).dropna().astype("Int64")

    # 2) Load pitcher_mega_z (prefer current pipeline output; fallback to final)
    mega_path = read_first_existing([
        "data/_projections/pitcher_mega_z.csv",
        "data/_projections/pitcher_mega_z_final.csv",
        "data/end_chain/final/pitcher_mega_z_final.csv",
    ])
    mega = pd.read_csv(mega_path)

    if "player_id" not in mega.columns:
        raise RuntimeError(f"{mega_path} missing 'player_id' column")

    mega_ids = to_int_series(mega["player_id"])
    mega_ids_set = set(int(x) for x in mega_ids.dropna().tolist())

    # 3) Build coverage table (mobile-friendly small CSV)
    coverage = pd.DataFrame({"player_id": starters_today.astype("Int64")})
    coverage["in_todaysgames"] = 1
    coverage["in_mega_z"] = coverage["player_id"].apply(lambda x: 1 if int(x) in mega_ids_set else 0)

    cov_path = SUM_DIR / "mega_z_starter_coverage.csv"
    coverage.sort_values("player_id").to_csv(cov_path, index=False)

    # 4) Fail fast if any starter is missing from mega_z
    missing = coverage.loc[coverage["in_mega_z"] == 0, "player_id"].astype("Int64")
    if not missing.empty:
        miss_df = pd.DataFrame({"player_id": missing})
        miss_path = SUM_DIR / "mega_z_starter_missing.csv"
        miss_df.to_csv(miss_path, index=False)
        raise RuntimeError(
            f"Starter coverage failure: {len(missing)} starter(s) absent in pitcher_mega_z. "
            f"See {cov_path} and {miss_path}."
        )

    # Minimal success breadcrumb (keeps your step logs clean)
    (SUM_DIR / "mega_z_starter_missing.csv").unlink(missing_ok=True)
    print(f"Starter coverage OK. Wrote {cov_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Make sure CI captures an error artifact
        (SUM_DIR / "mega_z_starter_coverage_error.txt").write_text(repr(e), encoding="utf-8")
        raise
