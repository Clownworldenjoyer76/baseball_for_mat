#!/usr/bin/env python3
# Purpose: Produce data/end_chain/final/pitcher_props_projected_final.csv with valid game/team context
# Inputs:
#   - data/_projections/pitcher_props_projected.csv         (projected pitcher props; MUST have player_id, game_id)
#   - data/raw/startingpitchers_with_opp_context.csv        (starters long; MUST have player_id, game_id, team_id, opponent_team_id)
# Output:
#   - data/end_chain/final/pitcher_props_projected_final.csv

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

PROJ_IN   = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SP_LONG   = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
OUT_DIR   = ROOT / "data" / "end_chain" / "final"
OUT_FILE  = OUT_DIR / "pitcher_props_projected_final.csv"
SUM_DIR   = ROOT / "summaries" / "07_final"
SUM_DIR.mkdir(parents=True, exist_ok=True)

VERSION = "v5-final-merge-on-playerid+gameid"

REQ_PROJ_COLS = ["player_id", "game_id"]
REQ_SP_COLS   = ["player_id", "game_id", "team_id", "opponent_team_id"]
KEEP_CTX      = ["game_id", "team_id", "opponent_team_id"]

def log(msg: str) -> None:
    print(msg, flush=True)

def must_have(df: pd.DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing required column(s): {missing}")

def to_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")

def main() -> int:
    log(f">> START: project_pitcher_props.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[project_pitcher_props] VERSION={VERSION} @ {Path(__file__).resolve()}")

    if not PROJ_IN.exists():
        raise FileNotFoundError(f"Missing input: {PROJ_IN}")
    if not SP_LONG.exists():
        raise FileNotFoundError(f"Missing input: {SP_LONG}")

    # Load
    proj = pd.read_csv(PROJ_IN, low_memory=False)
    sp   = pd.read_csv(SP_LONG, low_memory=False)

    # Validate required columns
    must_have(proj, REQ_PROJ_COLS, str(PROJ_IN))
    must_have(sp,   REQ_SP_COLS,   str(SP_LONG))

    # Key dtypes aligned for join
    proj = to_str(proj, ["player_id", "game_id"])
    sp   = to_str(sp,   ["player_id", "game_id", "team_id", "opponent_team_id"])

    # De-dup SP context on (player_id, game_id)
    sp_ctx = (
        sp[REQ_SP_COLS]
        .drop_duplicates(subset=["player_id", "game_id"])
        .reset_index(drop=True)
    )

    # Detect multi-matches in SP (bad upstream)
    dup_keys = (
        sp[["player_id","game_id"]]
        .value_counts()
        .reset_index(name="rows")
        .query("rows > 1")
    )
    if not dup_keys.empty:
        write_csv(dup_keys, SUM_DIR / "pitcher_context_duplicates.csv")
        raise RuntimeError("Context file has duplicate (player_id, game_id) rows; see summaries/07_final/pitcher_context_duplicates.csv")

    # Merge on (player_id, game_id)
    merged = proj.merge(
        sp_ctx[["player_id","game_id","team_id","opponent_team_id"]],
        on=["player_id","game_id"],
        how="left",
        suffixes=("", "_sp")
    )

    # Validate context coverage
    need = ["team_id","opponent_team_id"]
    bad = merged[merged[need].isna().any(axis=1)].copy()
    if not bad.empty:
        write_csv(bad[["player_id","game_id"] + [c for c in need if c in bad.columns]].drop_duplicates(),
                  SUM_DIR / "missing_pitcher_context_after_merge.csv")
        raise RuntimeError("Missing team_id/opponent_team_id after merge; see summaries/07_final/missing_pitcher_context_after_merge.csv")

    # Clean any *_sp or duplicate artifacts for ctx columns; keep a single clean set
    drop_cols = [c for c in merged.columns if c.endswith("_sp")]
    if drop_cols:
        merged = merged.drop(columns=drop_cols)

    # Optional: ensure ctx column types are strings for downstream key joins
    merged = to_str(merged, KEEP_CTX)

    # Write final artifact (do NOT overwrite source)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(merged, OUT_FILE)
    log(f"WROTE: {OUT_FILE} rows={len(merged)}")

    # Minimal status
    write_text(SUM_DIR / "status_project_pitchers.txt", f"OK project_pitcher_props.py rows={len(merged)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        # Emit error artifact
        write_text(SUM_DIR / "status_project_pitchers.txt", "FAIL project_pitcher_props.py")
        write_text(SUM_DIR / "errors_project_pitchers.txt", repr(e))
        print(e)
        sys.exit(1)
