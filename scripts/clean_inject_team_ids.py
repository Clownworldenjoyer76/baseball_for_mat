#!/usr/bin/env python3
# scripts/clean_inject_team_ids.py
#
# Tasks:
# 1) Create team_id in data/_projections/batter_props_projected_final.csv
#    by matching "team" to data/manual/team_directory.csv["team_name"].
# 2) Create team_id in data/_projections/batter_props_expanded_final.csv
#    by matching "team" to data/manual/team_directory.csv["team_name"].
# 3) Inject team_id values into data/_projections/pitcher_mega_z_final.csv
#    by matching "team" to data/manual/team_directory.csv["team_name"].
#
# Overwrites the input files in place.

from pathlib import Path
import sys
import pandas as pd

# ==== Filepaths ====
TEAM_DIR_FILE = Path("data/manual/team_directory.csv")

BATTERS_PROJECTED_FILE = Path("data/_projections/batter_props_projected_final.csv")
BATTERS_EXPANDED_FILE  = Path("data/_projections/batter_props_expanded_final.csv")
PITCHER_MEGA_FILE      = Path("data/_projections/pitcher_mega_z_final.csv")

REQUIRED_DIR_COLS = {"team_name", "team_id"}
REQUIRED_INPUT_COL = "team"

def load_team_directory(path: Path) -> pd.DataFrame:
    if not path.exists():
        sys.stderr.write(f"ERROR: Missing team directory file: {path}\n")
        sys.exit(1)
    df = pd.read_csv(path)
    missing = REQUIRED_DIR_COLS - set(df.columns)
    if missing:
        sys.stderr.write(f"ERROR: {path} missing required columns: {sorted(missing)}\n")
        sys.exit(1)
    return df[["team_name", "team_id"]]

def merge_team_id(input_path: Path, team_dir: pd.DataFrame, mode: str) -> None:
    """
    mode:
      - 'create': create/overwrite team_id for batter files
      - 'inject': fill missing team_id for pitcher mega file, or create if absent
    """
    if not input_path.exists():
        sys.stderr.write(f"ERROR: Missing input file: {input_path}\n")
        sys.exit(1)

    df = pd.read_csv(input_path)

    if REQUIRED_INPUT_COL not in df.columns:
        sys.stderr.write(f"ERROR: {input_path} missing required column: '{REQUIRED_INPUT_COL}'\n")
        sys.exit(1)

    # Left-join on exact match team -> team_name
    merged = df.merge(
        team_dir.rename(columns={"team_name": "team"}),
        on="team",
        how="left",
        suffixes=("", "_dir")
    )

    # Determine new team_id values from directory
    if "team_id" not in merged.columns:
        merged["team_id"] = merged["team_id_dir"]
    else:
        if mode == "create":
            merged["team_id"] = merged["team_id_dir"]
        elif mode == "inject":
            merged["team_id"] = merged["team_id"].where(merged["team_id"].notna(), merged["team_id_dir"])
        else:
            sys.stderr.write(f"ERROR: Unknown mode '{mode}' for {input_path}\n")
            sys.exit(1)

    # Report unmatched teams (no team_id found)
    unmatched = merged["team_id"].isna().sum()
    if unmatched > 0:
        sys.stderr.write(
            f"WARNING: {unmatched} row(s) in {input_path} could not map 'team' to team_id via {TEAM_DIR_FILE}\n"
        )

    # Drop helper column and write back
    if "team_id_dir" in merged.columns:
        merged = merged.drop(columns=["team_id_dir"])

    merged.to_csv(input_path, index=False)
    print(f"✅ Updated team_id in {input_path} (rows={len(merged)}, unmatched={unmatched})")

def main():
    team_dir = load_team_directory(TEAM_DIR_FILE)

    # 1) batter_props_projected_final.csv -> create team_id
    merge_team_id(BATTERS_PROJECTED_FILE, team_dir, mode="create")

    # 2) batter_props_expanded_final.csv -> create team_id
    merge_team_id(BATTERS_EXPANDED_FILE, team_dir, mode="create")

    # 3) pitcher_mega_z_final.csv -> inject team_id
    merge_team_id(PITCHER_MEGA_FILE, team_dir, mode="inject")

if __name__ == "__main__":
    main()
