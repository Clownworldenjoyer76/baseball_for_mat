#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pitcher_mega_z.py

Goal:
- Build pitcher_mega_z strictly keyed on player_id.
- Guarantee all starters from todaysgames_normalized_fixed.csv appear (by player_id) in the output.
- Do NOT backfill stats. If a starter isn't present upstream, append a skeletal row with only player_id.
- Emit diagnostics so coverage issues are visible but non-fatal.

Inputs:
- data/cleaned/pitchers_normalized_cleaned.csv   (primary mega source; must have player_id)
- data/_projections/todaysgames_normalized_fixed.csv (for today's starter player_ids)

Output:
- data/_projections/pitcher_mega_z.csv
- summaries/projections/mega_z_starter_coverage.csv
- summaries/projections/mega_z_starter_missing.csv  (may be empty if all covered)
- summaries/projections/mega_z_build_log.txt
"""

from pathlib import Path
import pandas as pd
import numpy as np

# Paths
ROOT = Path(".")
IN_MEGA = ROOT / "data" / "cleaned" / "pitchers_normalized_cleaned.csv"
IN_TODAY = ROOT / "data" / "_projections" / "todaysgames_normalized_fixed.csv"
OUT_MEGA = ROOT / "data" / "_projections" / "pitcher_mega_z.csv"

SUM_DIR = ROOT / "summaries" / "projections"
SUM_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = SUM_DIR / "mega_z_build_log.txt"
COVER_FILE = SUM_DIR / "mega_z_starter_coverage.csv"
MISS_FILE = SUM_DIR / "mega_z_starter_missing.csv"

# Helper IO
def wlog(msg: str):
    print(msg, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")

def require_cols(df: pd.DataFrame, cols, name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing columns: {missing}")

def to_num(series: pd.Series):
    return pd.to_numeric(series, errors="coerce")

def main():
    # fresh log
    LOG_FILE.write_text("", encoding="utf-8")
    wlog("=== pitcher_mega_z.py START ===")

    # ---------- Load inputs ----------
    if not IN_MEGA.exists():
        raise FileNotFoundError(f"Missing mega source: {IN_MEGA}")
    if not IN_TODAY.exists():
        raise FileNotFoundError(f"Missing todaysgames file: {IN_TODAY}")

    mega = pd.read_csv(IN_MEGA)
    require_cols(mega, ["player_id"], str(IN_MEGA))

    today = pd.read_csv(IN_TODAY)
    # today must provide starter ids
    require_cols(
        today,
        ["pitcher_home_id", "pitcher_away_id", "game_id"],
        str(IN_TODAY),
    )

    # ---------- Normalize dtypes (strictly numeric player_id) ----------
    mega["player_id"] = to_num(mega["player_id"]).astype("Int64")
    # Drop rows without player_id
    before = len(mega)
    mega = mega.dropna(subset=["player_id"]).copy()
    after = len(mega)
    if after < before:
        wlog(f"Pruned {before - after} rows in mega with null player_id.")

    # Deduplicate on player_id (keep first occurrence)
    mega = mega.sort_index().drop_duplicates(subset=["player_id"], keep="first").copy()

    # Collect today starters
    pid_cols = ["pitcher_home_id", "pitcher_away_id"]
    for c in pid_cols:
        today[c] = to_num(today[c]).astype("Int64")

    starters = pd.unique(
        pd.concat([today["pitcher_home_id"], today["pitcher_away_id"]], ignore_index=True)
    )
    starters = pd.Series(starters, name="player_id").dropna().astype("Int64")
    starters = starters[starters.notna()]  # safety
    starters = starters.unique()

    wlog(f"Loaded mega rows: {len(mega)}")
    wlog(f"Unique starter ids today: {len(starters)}")

    # ---------- Coverage check (by player_id only) ----------
    mega_ids = set(mega["player_id"].dropna().astype(int).tolist())
    missing_ids = [int(x) for x in starters if int(x) not in mega_ids]

    # Write coverage summary
    cov = pd.DataFrame({
        "metric": ["mega_rows", "starters_today", "starters_in_mega", "starters_missing"],
        "value": [len(mega), len(starters), len(starters) - len(missing_ids), len(missing_ids)],
    })
    cov.to_csv(COVER_FILE, index=False)

    if missing_ids:
        pd.DataFrame({"player_id": missing_ids}).to_csv(MISS_FILE, index=False)
        wlog(f"Starter coverage: {len(missing_ids)} missing from mega source. See {MISS_FILE.name}")
    else:
        # ensure an empty file exists for parity
        pd.DataFrame(columns=["player_id"]).to_csv(MISS_FILE, index=False)
        wlog("Starter coverage: all starters present in mega source.")

    # ---------- Enforce coverage without backfilling stats ----------
    # If a starter is missing, append a skeletal row with only player_id (other cols NaN).
    if missing_ids:
        # Determine all columns mega currently has
        cols = list(mega.columns)
        # Build empty rows for each missing starter
        add = pd.DataFrame({c: [pd.NA] * len(missing_ids) for c in cols})
        add["player_id"] = pd.Series(missing_ids, dtype="Int64")
        wlog(f"Appending {len(add)} skeletal starter row(s) (player_id only). No stats imputed.")

        mega = pd.concat([mega, add], ignore_index=True)
        # Re-deduplicate just in case
        mega = mega.drop_duplicates(subset=["player_id"], keep="first").reset_index(drop=True)

    # ---------- Final sanity: still no null player_id ----------
    if mega["player_id"].isna().any():
        bad = mega.loc[mega["player_id"].isna()]
        tmp = SUM_DIR / "mega_z_null_player_id_rows.csv"
        bad.to_csv(tmp, index=False)
        raise RuntimeError(f"Null player_id detected in output; see {tmp}")

    # ---------- Write output ----------
    OUT_MEGA.parent.mkdir(parents=True, exist_ok=True)
    mega.to_csv(OUT_MEGA, index=False)
    wlog(f"Wrote mega_z: {len(mega)} rows -> {OUT_MEGA}")

    wlog("=== pitcher_mega_z.py DONE ===")

if __name__ == "__main__":
    main()
