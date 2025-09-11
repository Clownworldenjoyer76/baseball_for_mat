#!/usr/bin/env python3
# Stage: 06
# Goal: Produce enriched pitcher projections with valid (game_id, team_id, opponent_team_id)
# Inputs:
#   data/_projections/pitcher_props_projected.csv          [needs: player_id, optional: game_id]
#   data/raw/startingpitchers_with_opp_context.csv         [needs: player_id, game_id, team_id, opponent_team_id]
# Outputs:
#   data/end_chain/final/pitcher_props_projected_final.csv
#   data/_projections/pitcher_props_projected.csv          (overwritten with enriched rows)

from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

PROJ_SRC_IN   = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SP_LONG       = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
OUT_FILE_FINAL= ROOT / "data" / "end_chain" / "final" / "pitcher_props_projected_final.csv"
OUT_FILE_PROJ = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SUM_DIR       = ROOT / "summaries" / "06_projection"

VERSION = "v13-normalize_player_id-inject-gid-merge-clean"

REQ_PROJ_COLS = ["player_id"]
REQ_SP_COLS   = ["player_id", "game_id", "team_id", "opponent_team_id"]
JOIN_KEYS     = ["player_id", "game_id"]
CTX_COLS      = ["team_id", "opponent_team_id"]

def log(msg: str) -> None:
    print(msg, flush=True)

def must_have(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing required column(s): {missing}")

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def norm_pid_series(s: pd.Series) -> pd.Series:
    """Normalize player_id to plain string (strip .0, whitespace, NA -> '')"""
    s = s.astype("string")
    s = s.str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.fillna("")
    return s

def main() -> int:
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log(f">> START: project_pitcher_props.py {VERSION} ({ts})")
    log(f"[PATH] PROJ_SRC_IN={PROJ_SRC_IN}")
    log(f"[PATH] SP_LONG={SP_LONG}")
    log(f"[PATH] OUT_FILE_FINAL={OUT_FILE_FINAL}")
    log(f"[PATH] OUT_FILE_PROJ={OUT_FILE_PROJ}")

    if not PROJ_SRC_IN.exists():
        raise FileNotFoundError(str(PROJ_SRC_IN))
    if not SP_LONG.exists():
        raise FileNotFoundError(str(SP_LONG))

    proj = pd.read_csv(PROJ_SRC_IN, dtype=str, low_memory=False)
    sp   = pd.read_csv(SP_LONG,     dtype=str, low_memory=False)

    must_have(proj, REQ_PROJ_COLS, str(PROJ_SRC_IN))
    must_have(sp,   REQ_SP_COLS,   str(SP_LONG))

    # Normalize ids / keys
    proj["player_id"] = norm_pid_series(proj["player_id"])
    if "game_id" in proj.columns:
        proj["game_id"] = proj["game_id"].astype("string").fillna("").str.strip()
    else:
        proj["game_id"] = ""

    sp["player_id"]        = norm_pid_series(sp["player_id"])
    sp["game_id"]          = sp["game_id"].astype("string").fillna("").str.strip()
    sp["team_id"]          = sp["team_id"].astype("string").fillna("").str.strip()
    sp["opponent_team_id"] = sp["opponent_team_id"].astype("string").fillna("").str.strip()

    # Validate projections have player_id
    miss_pid = proj[proj["player_id"] == ""]
    if not miss_pid.empty:
        write_csv(miss_pid, SUM_DIR / "pitcher_proj_missing_player_id.csv")
        raise RuntimeError("Projection rows missing player_id; see summaries/06_projection/pitcher_proj_missing_player_id.csv")

    # Build (player_id -> resolved game_id) from starters; choose lowest numeric if multiple
    sp_keys = sp.loc[sp["player_id"] != "", ["player_id", "game_id"]].drop_duplicates()
    # numeric sort key; fallback to lexicographic
    sp_keys["_gid_num"] = pd.to_numeric(sp_keys["game_id"], errors="coerce")
    sp_keys = sp_keys.sort_values(["player_id", "_gid_num", "game_id"])
    sp_keys_resolved = sp_keys.drop_duplicates(subset=["player_id"], keep="first")[["player_id", "game_id"]].rename(columns={"game_id":"game_id_from_sp"})

    # Inject game_id where missing in projections
    needs_gid = proj["game_id"].eq("") | proj["game_id"].eq("UNKNOWN")
    if needs_gid.any():
        proj = proj.merge(sp_keys_resolved, on="player_id", how="left")
        proj.loc[needs_gid, "game_id"] = proj.loc[needs_gid, "game_id_from_sp"].fillna("")
        proj = proj.drop(columns=[c for c in ["game_id_from_sp"] if c in proj.columns])

    # Post-injection check
    still_missing_gid = proj["game_id"].eq("") | proj["game_id"].eq("UNKNOWN")
    if still_missing_gid.any():
        write_csv(proj.loc[still_missing_gid, ["player_id", "game_id"]], SUM_DIR / "missing_gameid_injected_for_proj.csv")
        # Don't fail; log only. Downstream context merge will leave UNKNOWN for these.

    # Prepare unique starters context on (player_id, game_id)
    sp_ctx = sp[JOIN_KEYS + CTX_COLS].drop_duplicates()
    dup_ctx = sp_ctx.groupby(JOIN_KEYS, as_index=False).size()
    dup_ctx = dup_ctx[dup_ctx["size"] > 1]
    if not dup_ctx.empty:
        write_csv(dup_ctx, SUM_DIR / "pitcher_context_duplicates.csv")
        # Keep first occurrence deterministically
        sp_ctx = (sp_ctx
                  .assign(_gid_num=pd.to_numeric(sp_ctx["game_id"], errors="coerce"))
                  .sort_values(["player_id", "_gid_num", "game_id"])
                  .drop(columns="_gid_num")
                  .drop_duplicates(subset=JOIN_KEYS, keep="first"))

    # Merge full context
    merged = proj.merge(sp_ctx, on=JOIN_KEYS, how="left", suffixes=("", "_ctx"))

    # Fill missing context with "UNKNOWN"
    for c in CTX_COLS:
        if c not in merged.columns:
            merged[c] = "UNKNOWN"
        merged[c] = merged[c].astype("string").fillna("UNKNOWN")

    # Drop any *_x/*_y/*_ctx artifacts if slipped in
    merged = merged[[c for c in merged.columns if not (c.endswith("_x") or c.endswith("_y") or c.endswith("_ctx"))]]

    # Ensure strings, no NaN
    for c in merged.columns:
        merged[c] = merged[c].astype("string").fillna("")

    # Write outputs (final + overwrite projections with enriched rows)
    write_csv(merged, OUT_FILE_FINAL)
    write_csv(merged, OUT_FILE_PROJ)

    # Tiny status line
    rows_proj_total = len(proj)
    rows_out_final  = len(merged)
    rows_sp_input   = len(sp)
    log(f"[OK] WROTE {OUT_FILE_FINAL} and {OUT_FILE_PROJ} rows={rows_out_final} | rows_proj_total={rows_proj_total} rows_sp_input={rows_sp_input}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        try:
            write_text(SUM_DIR / "project_pitcher_props_error.txt", f"{e!r}")
        except Exception:
            pass
        print(e)
        sys.exit(1)
