#!/usr/bin/env python3
# Purpose: Produce data/end_chain/final/pitcher_props_projected_final.csv WITH VALID CONTEXT.
# Strategy:
#   1) Inject game_id into pitcher projections using starters context (by player_id).
#   2) Merge full context using (player_id, game_id).
#   3) Fail fast on duplicates/ambiguity/missing context.
#
# Inputs (REQUIRED SCHEMA):
#   - data/_projections/pitcher_props_projected.csv
#       required: player_id
#       optional (ignored for context): any *_x/_y/_sp, team_id_x/opponent_team_id_x/home_away
#   - data/raw/startingpitchers_with_opp_context.csv
#       required: player_id, game_id, team_id, opponent_team_id
#
# Output:
#   - data/end_chain/final/pitcher_props_projected_final.csv
# Diagnostics:
#   - summaries/07_final/pitcher_proj_missing_player_id.csv
#   - summaries/07_final/pitcher_proj_gameid_injected_preview.csv
#   - summaries/07_final/pitcher_proj_duplicate_gameids.csv
#   - summaries/07_final/pitcher_context_duplicates.csv
#   - summaries/07_final/missing_pitcher_context_after_merge.csv
#   - summaries/07_final/status_project_pitchers.txt
#   - summaries/07_final/errors_project_pitchers.txt

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

VERSION = "v7-inject_gameid_then_merge"

REQ_PROJ_COLS = ["player_id"]
REQ_SP_COLS   = ["player_id", "game_id", "team_id", "opponent_team_id"]
JOIN_KEYS     = ["player_id", "game_id"]
CTX_COLS      = ["team_id", "opponent_team_id"]

def log(msg: str) -> None:
    print(msg, flush=True)

def must_have(df: pd.DataFrame, cols: list[str], name: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing required column(s): {miss}")

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

    proj = pd.read_csv(PROJ_IN, low_memory=False)
    sp   = pd.read_csv(SP_LONG, low_memory=False)

    must_have(proj, REQ_PROJ_COLS, str(PROJ_IN))
    must_have(sp,   REQ_SP_COLS,   str(SP_LONG))

    # Normalize types
    proj = to_str(proj, ["player_id"])
    sp   = to_str(sp,   ["player_id", "game_id", "team_id", "opponent_team_id"])

    # Fail if any projections missing player_id
    miss_pid = proj[proj["player_id"].isna() | (proj["player_id"] == "")]
    if not miss_pid.empty:
        write_csv(miss_pid, SUM_DIR / "pitcher_proj_missing_player_id.csv")
        raise RuntimeError("Projection rows missing player_id; see summaries/07_final/pitcher_proj_missing_player_id.csv")

    # STEP 1: Inject game_id by player_id (each starter must map to exactly ONE game_id)
    # De-dup starters on (player_id, game_id); then enforce uniqueness of player_id -> single game_id
    sp_keys = sp[["player_id", "game_id"]].drop_duplicates()

    dup_gameids = (
        sp_keys.value_counts(subset=["player_id"])
        .reset_index(name="rows")
        .query("rows > 1")
        .merge(sp_keys, on="player_id", how="left")
        .sort_values(["player_id", "game_id"])
    )
    if not dup_gameids.empty:
        write_csv(dup_gameids, SUM_DIR / "pitcher_proj_duplicate_gameids.csv")
        raise RuntimeError("A player_id maps to multiple game_id in starters context; see summaries/07_final/pitcher_proj_duplicate_gameids.csv")

    # Inject game_id -> projection
    proj_with_gid = proj.merge(sp_keys, on="player_id", how="left", suffixes=("", "_sp"))
    write_csv(proj_with_gid.head(30), SUM_DIR / "pitcher_proj_gameid_injected_preview.csv")

    if proj_with_gid["game_id"].isna().any():
        bad = proj_with_gid[proj_with_gid["game_id"].isna()].copy()
        write_csv(bad, SUM_DIR / "missing_gameid_injected_for_proj.csv")
        raise RuntimeError("Failed to inject game_id for some projections; see summaries/07_final/missing_gameid_injected_for_proj.csv")

    proj_with_gid = to_str(proj_with_gid, ["game_id"])

    # STEP 2: Merge full context using (player_id, game_id)
    # Enforce uniqueness in full context
    dup_ctx = (
        sp.value_counts(subset=JOIN_KEYS)
        .reset_index(name="rows")
        .query("rows > 1")
    )
    if not dup_ctx.empty:
        write_csv(dup_ctx, SUM_DIR / "pitcher_context_duplicates.csv")
        raise RuntimeError("Duplicate (player_id, game_id) rows in starters context; see summaries/07_final/pitcher_context_duplicates.csv")

    sp_ctx = sp[JOIN_KEYS + CTX_COLS].drop_duplicates()

    merged = proj_with_gid.merge(
        sp_ctx,
        on=JOIN_KEYS,
        how="left",
        suffixes=("", "_ctx")
    )

    # Validate complete context
    needs = ["game_id"] + CTX_COLS
    missing_any = merged[needs].isna().any(axis=1)
    if missing_any.any():
        bad = merged.loc[missing_any, ["player_id"] + needs].drop_duplicates()
        write_csv(bad, SUM_DIR / "missing_pitcher_context_after_merge.csv")
        raise RuntimeError("Missing game_id/team_id/opponent_team_id after merge; see summaries/07_final/missing_pitcher_context_after_merge.csv")

    # STEP 3: Clean duplicate artifacts
    drop_cols = [c for c in merged.columns if c.endswith(("_x", "_y", "_sp", "_ctx"))]
    if drop_cols:
        merged = merged.drop(columns=drop_cols)

    # Write final
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
