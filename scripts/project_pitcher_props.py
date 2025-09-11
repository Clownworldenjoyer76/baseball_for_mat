#!/usr/bin/env python3
# Produce data/end_chain/final/pitcher_props_projected_final.csv with valid (game_id, team_id, opponent_team_id).
# Stage: 06
# Inputs (required schema):
#   - data/_projections/pitcher_props_projected.csv
#       required: player_id
#       optional: game_id
#   - data/raw/startingpitchers_with_opp_context.csv
#       required: player_id, game_id, team_id, opponent_team_id
# Output:
#   - data/end_chain/final/pitcher_props_projected_final.csv
# Behavior:
#   - Inject game_id into projections by player_id using starters context.
#   - If a player_id maps to multiple game_id, resolve deterministically (lowest numeric game_id), and LOG ONLY.
#   - Merge full context on (player_id, game_id).
#   - Fail if any merged row lacks team_id/opponent_team_id.
#   - No *_x/*_y/*_sp/*_ctx artifacts in output.

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
SUM_DIR  = ROOT / "summaries" / "06_pitcher_props"  # stage-06 diagnostics

VERSION = "v10-06stage-inject-gid-resolve-dupes-merge-context"

REQ_PROJ_COLS = ["player_id"]
REQ_SP_COLS   = ["player_id", "game_id", "team_id", "opponent_team_id"]
JOIN_KEYS     = ["player_id", "game_id"]
CTX_COLS      = ["team_id", "opponent_team_id"]

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
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    print(f">> START: project_pitcher_props.py {VERSION} ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")
    print(f"[PATH] PROJ_IN={PROJ_IN}")
    print(f"[PATH] SP_LONG={SP_LONG}")
    print(f"[PATH] OUT_FILE={OUT_FILE}")

    if not PROJ_IN.exists():
        raise FileNotFoundError(str(PROJ_IN))
    if not SP_LONG.exists():
        raise FileNotFoundError(str(SP_LONG))

    proj = pd.read_csv(PROJ_IN, low_memory=False)
    sp   = pd.read_csv(SP_LONG, low_memory=False)

    must_have(proj, REQ_PROJ_COLS, str(PROJ_IN))
    must_have(sp,   REQ_SP_COLS,   str(SP_LONG))

    proj = to_str(proj, ["player_id", "game_id"])
    sp   = to_str(sp,   ["player_id", "game_id", "team_id", "opponent_team_id"])

    # Validate projections have player_id
    miss_pid = proj[proj["player_id"].isna() | (proj["player_id"].str.len() == 0)]
    if not miss_pid.empty:
        write_csv(miss_pid, SUM_DIR / "pitcher_proj_missing_player_id.csv")
        raise RuntimeError("Projection rows missing player_id; see summaries/06_pitcher_props/pitcher_proj_missing_player_id.csv")

    # Build starters key map (exclude UNKNOWN/NaN game_id)
    sp_keys = sp.loc[sp["game_id"].notna() & (sp["game_id"] != "UNKNOWN"), ["player_id", "game_id"]].drop_duplicates()

    # LOG duplicates per player_id (more than one UNIQUE game_id), but DO NOT FAIL; resolve deterministically.
    dup_map = (
        sp_keys.groupby("player_id", as_index=False)["game_id"].nunique()
        .rename(columns={"game_id": "unique_game_id_count"})
        .query("unique_game_id_count > 1")[["player_id"]]
    )
    if not dup_map.empty:
        write_csv(
            sp_keys.merge(dup_map, on="player_id", how="inner").sort_values(["player_id", "game_id"]),
            SUM_DIR / "pitcher_proj_duplicate_gameids.csv"
        )

    # Resolve to a single game_id per player_id: lowest numeric; fallback to lexicographic
    sp_keys_resolved = (
        sp_keys.assign(_gid_num=pd.to_numeric(sp_keys["game_id"], errors="coerce"))
               .sort_values(["player_id", "_gid_num", "game_id"])
               .drop(columns=["_gid_num"])
               .drop_duplicates(subset=["player_id"], keep="first")
               .reset_index(drop=True)
    )

    # Inject game_id into projections where missing/blank/UNKNOWN
    needs_gid = proj["game_id"].isna() | (proj["game_id"] == "") | (proj["game_id"] == "UNKNOWN")
    if needs_gid.any():
        proj = proj.merge(sp_keys_resolved, on="player_id", how="left", suffixes=("", "_from_sp"))
        proj.loc[needs_gid, "game_id"] = proj.loc[needs_gid, "game_id_from_sp"]
        proj = proj.drop(columns=[c for c in ["game_id_from_sp"] if c in proj.columns])

    # Validate all projections now have game_id
    miss_gid = proj[proj["game_id"].isna() | (proj["game_id"] == "") | (proj["game_id"] == "UNKNOWN")]
    if not miss_gid.empty:
        write_csv(miss_gid[["player_id", "game_id"]], SUM_DIR / "missing_gameid_injected_for_proj.csv")
        raise RuntimeError("Failed to inject game_id for some projections; see summaries/06_pitcher_props/missing_gameid_injected_for_proj.csv")

    proj = to_str(proj, ["game_id"])

    # Ensure starters context is unique on (player_id, game_id)
    sp_ctx = sp[JOIN_KEYS + CTX_COLS].drop_duplicates()
    dup_ctx = (
        sp_ctx.groupby(JOIN_KEYS, as_index=False).size().query("size > 1")
    )
    if not dup_ctx.empty:
        write_csv(dup_ctx, SUM_DIR / "pitcher_context_duplicates.csv")
        raise RuntimeError("Duplicate (player_id, game_id) in starters context; see summaries/06_pitcher_props/pitcher_context_duplicates.csv")

    # Merge full context
    merged = proj.merge(sp_ctx, on=JOIN_KEYS, how="left", suffixes=("", "_ctx"))

    # Validate context present
    missing_ctx = merged[CTX_COLS].isna().any(axis=1)
    if missing_ctx.any():
        bad = merged.loc[missing_ctx, ["player_id", "game_id"] + CTX_COLS].drop_duplicates()
        write_csv(bad, SUM_DIR / "missing_pitcher_context_after_merge.csv")
        raise RuntimeError("Missing team_id/opponent_team_id after merge; see summaries/06_pitcher_props/missing_pitcher_context_after_merge.csv")

    # Drop merge artifacts
    merged = merged.drop(columns=[c for c in merged.columns if c.endswith(("_x", "_y", "_sp", "_ctx"))])

    # Write final
    write_csv(merged, OUT_FILE)
    write_text(SUM_DIR / "status_project_pitchers.txt", f"OK project_pitcher_props.py rows={len(merged)}")
    print(f"[OK] WROTE {OUT_FILE} rows={len(merged)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        write_text(SUM_DIR / "status_project_pitchers.txt", "FAIL project_pitcher_props.py")
        write_text(SUM_DIR / "errors_project_pitchers.txt", repr(e))
        print(e)
        sys.exit(1)
