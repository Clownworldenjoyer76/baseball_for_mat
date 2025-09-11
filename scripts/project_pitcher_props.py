#!/usr/bin/env python3
# Purpose: Produce data/end_chain/final/pitcher_props_projected_final.csv with VALID game/team context.
# Inputs (REQUIRED SCHEMA):
#   - data/_projections/pitcher_props_projected.csv
#       required: player_id, game_id
#   - data/raw/startingpitchers_with_opp_context.csv
#       required: player_id, game_id, team_id, opponent_team_id
# Output:
#   - data/end_chain/final/pitcher_props_projected_final.csv
# Behavior:
#   - Merge strictly on (player_id, game_id)
#   - Fail if any required context is missing after merge
#   - Remove *_x/*_y/*_sp duplicate artifacts

from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

PROJ_IN  = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SP_LONG  = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
OUT_DIR  = ROOT / "data" / "end_chain" / "final"
OUT_FILE = OUT_DIR / "pitcher_props_projected_final.csv"
SUM_DIR  = ROOT / "summaries" / "07_final"

VERSION = "v6-merge_on_playerid_gameid_fail_fast"

REQ_PROJ_COLS = ["player_id", "game_id"]
REQ_SP_COLS   = ["player_id", "game_id", "team_id", "opponent_team_id"]
CTX_COLS      = ["team_id", "opponent_team_id"]
JOIN_KEYS     = ["player_id", "game_id"]

def log(msg: str) -> None:
    print(msg, flush=True)

def must_have(df: pd.DataFrame, cols: list[str], name: str) -> None:
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def main() -> int:
    log(f">> START: project_pitcher_props.py {VERSION} ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    log(f"[PATH] PROJ_IN={PROJ_IN}")
    log(f"[PATH] SP_LONG={SP_LONG}")
    log(f"[PATH] OUT_FILE={OUT_FILE}")

    if not PROJ_IN.exists():
        raise FileNotFoundError(f"Missing input: {PROJ_IN}")
    if not SP_LONG.exists():
        raise FileNotFoundError(f"Missing input: {SP_LONG}")

    # Load
    proj = pd.read_csv(PROJ_IN, low_memory=False)
    sp   = pd.read_csv(SP_LONG, low_memory=False)

    # Validate required schema
    must_have(proj, REQ_PROJ_COLS, str(PROJ_IN))
    must_have(sp,   REQ_SP_COLS,   str(SP_LONG))

    # Align dtypes for join keys and context
    proj = to_str(proj, JOIN_KEYS)
    sp   = to_str(sp,   JOIN_KEYS + CTX_COLS)

    # Fail early on null join keys
    null_proj = proj[proj[JOIN_KEYS].isna().any(axis=1)]
    null_sp   = sp[sp[JOIN_KEYS].isna().any(axis=1)]
    if not null_proj.empty:
        write_csv(null_proj, SUM_DIR / "pitcher_proj_null_join_keys.csv")
        raise RuntimeError("Null join keys in PROJ_IN; see summaries/07_final/pitcher_proj_null_join_keys.csv")
    if not null_sp.empty:
        write_csv(null_sp, SUM_DIR / "pitcher_sp_null_join_keys.csv")
        raise RuntimeError("Null join keys in SP_LONG; see summaries/07_final/pitcher_sp_null_join_keys.csv")

    # Ensure uniqueness of SP context by (player_id, game_id)
    dup_keys = (
        sp[JOIN_KEYS]
        .value_counts()
        .reset_index(name="rows")
        .query("rows > 1")
    )
    if not dup_keys.empty:
        write_csv(dup_keys, SUM_DIR / "pitcher_context_duplicates.csv")
        raise RuntimeError("Duplicate (player_id, game_id) in context; see summaries/07_final/pitcher_context_duplicates.csv")

    sp_ctx = sp[JOIN_KEYS + CTX_COLS].drop_duplicates()

    # Merge strictly on (player_id, game_id)
    merged = proj.merge(
        sp_ctx,
        on=JOIN_KEYS,
        how="left",
        suffixes=("", "_ctx")
    )

    # Validate context coverage — no UNKNOWN/NaN allowed
    missing_ctx = merged[CTX_COLS].isna().any(axis=1)
    if missing_ctx.any():
        bad = merged.loc[missing_ctx, JOIN_KEYS + CTX_COLS].drop_duplicates()
        write_csv(bad, SUM_DIR / "missing_pitcher_context_after_merge.csv")
        raise RuntimeError("Missing team_id/opponent_team_id after merge; see summaries/07_final/missing_pitcher_context_after_merge.csv")

    # Drop any accidental *_x/*_y/*_sp/*_ctx artifacts
    drop_cols = [c for c in merged.columns if c.endswith(("_x", "_y", "_sp", "_ctx"))]
    if drop_cols:
        merged = merged.drop(columns=drop_cols)

    # Final write
    write_csv(merged, OUT_FILE)
    write_text(SUM_DIR / "status_project_pitchers.txt", f"OK project_pitcher_props.py rows={len(merged)}")
    log(f"[OK] WROTE {OUT_FILE} rows={len(merged)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        write_text(SUM_DIR / "status_project_pitchers.txt", "FAIL project_pitcher_props.py")
        write_text(SUM_DIR / "errors_project_pitchers.txt", repr(e))
        print(e)
        sys.exit(1)
