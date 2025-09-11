#!/usr/bin/env python3
# Stage: 06
# Purpose: Produce /data/end_chain/final/pitcher_props_projected_final.csv containing ONLY today's starters
#          with valid (game_id, team_id, opponent_team_id) merged onto projections.
#
# Inputs (must exist):
#   - /data/_projections/pitcher_props_projected.csv
#       required cols: player_id
#       optional cols: game_id
#   - /data/raw/startingpitchers_with_opp_context.csv
#       required cols: player_id, game_id, team_id, opponent_team_id
#
# Output:
#   - /data/end_chain/final/pitcher_props_projected_final.csv
#
# Behavior (deterministic, no hard-fail on coverage):
#   - Normalize IDs in BOTH inputs (drop trailing ".0", keep 'UNKNOWN' literal).
#   - Resolve multiple game_id per player_id in starters by choosing the lowest numeric game_id (log-only).
#   - Inner-join projections to starters by normalized player_id to restrict to actual starters today.
#   - Overwrite/insert game_id from starters. Merge team_id/opponent_team_id by (player_id, game_id).
#   - Emit diagnostics under /summaries/06_projection, never raise for partial coverage.
#   - Only raise for missing input files or required columns.
#   - No *_x/*_y/*_sp/*_ctx columns in output.

from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]

PROJ_IN  = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SP_LONG  = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
OUT_DIR  = ROOT / "data" / "end_chain" / "final"
OUT_FILE = OUT_DIR / "pitcher_props_projected_final.csv"
SUM_DIR  = ROOT / "summaries" / "06_projection"  # aligned with workflow collection

VERSION = "v11-normalize_ids_restrict_to_starters_log_only"

REQ_PROJ_COLS = ["player_id"]
REQ_SP_COLS   = ["player_id", "game_id", "team_id", "opponent_team_id"]
JOIN_KEYS     = ["player_id", "game_id"]
CTX_COLS      = ["team_id", "opponent_team_id"]

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

def norm_id(val) -> str:
    s = str(val).strip()
    if s == "" or s.upper() == "UNKNOWN" or s.lower() == "nan":
        return "UNKNOWN"
    # try numeric canonicalization
    try:
        f = float(s)
        if np.isfinite(f) and float(int(f)) == f:
            return str(int(f))
        # non-integer numeric; keep as minimal string
        return s
    except Exception:
        if s.endswith(".0") and s[:-2].isdigit():
            return s[:-2]
        return s

def norm_series(sr: pd.Series) -> pd.Series:
    return sr.map(norm_id).astype("string")

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

    # Load as strings to preserve exact literals
    proj = pd.read_csv(PROJ_IN, dtype=str, low_memory=False)
    sp   = pd.read_csv(SP_LONG, dtype=str, low_memory=False)

    must_have(proj, REQ_PROJ_COLS, str(PROJ_IN))
    must_have(sp,   REQ_SP_COLS,   str(SP_LONG))

    # Normalize key columns
    proj["player_id_norm"] = norm_series(proj["player_id"])
    if "game_id" in proj.columns:
        proj["game_id_norm"] = norm_series(proj["game_id"])
    else:
        proj["game_id_norm"] = "UNKNOWN"

    sp["player_id_norm"] = norm_series(sp["player_id"])
    sp["game_id_norm"]   = norm_series(sp["game_id"])
    sp["team_id"]        = norm_series(sp["team_id"])
    sp["opponent_team_id"] = norm_series(sp["opponent_team_id"])

    # Ensure projections have player_id
    miss_pid = proj.loc[(proj["player_id_norm"] == "UNKNOWN")]
    if not miss_pid.empty:
        write_csv(miss_pid, SUM_DIR / "pitcher_proj_missing_player_id.csv")

    # Build starters key map and resolve multiple game_ids per player
    sp_keys = (
        sp.loc[(sp["player_id_norm"] != "UNKNOWN") & (sp["game_id_norm"] != "UNKNOWN"),
               ["player_id_norm", "game_id_norm"]]
        .drop_duplicates()
        .copy()
    )

    # Log duplicate game_ids per player_id (resolve deterministically)
    gid_counts = sp_keys.groupby("player_id_norm", as_index=False)["game_id_norm"].nunique()
    dup_players = gid_counts.loc[gid_counts["game_id_norm"] > 1, "player_id_norm"]
    if not dup_players.empty:
        dup_rows = sp_keys.merge(dup_players.to_frame(), on="player_id_norm", how="inner").sort_values(["player_id_norm", "game_id_norm"])
        write_csv(dup_rows, SUM_DIR / "pitcher_proj_duplicate_gameids.csv")

    # Resolve to lowest numeric game_id per player_id
    sp_keys["_gid_num"] = pd.to_numeric(sp_keys["game_id_norm"], errors="coerce")
    sp_keys_resolved = (
        sp_keys.sort_values(["player_id_norm", "_gid_num", "game_id_norm"])
               .drop_duplicates(subset=["player_id_norm"], keep="first")
               .drop(columns=["_gid_num"])
               .rename(columns={"game_id_norm": "resolved_game_id_norm"})
    )

    # Determine coverage (which projected pitchers are starters today)
    proj_only = set(proj["player_id_norm"])
    sp_only   = set(sp_keys_resolved["player_id_norm"])
    missing_in_sp = sorted(list(proj_only - sp_only))
    if missing_in_sp:
        write_csv(pd.DataFrame({"player_id": missing_in_sp}), SUM_DIR / "pitcher_proj_not_in_starters.csv")

    # Restrict to starters: inner join projections to resolved starters by player_id_norm
    proj_starters = proj.merge(sp_keys_resolved, on="player_id_norm", how="inner")

    # Overwrite/insert game_id from starters resolution
    proj_starters["game_id_norm"] = proj_starters["resolved_game_id_norm"]
    proj_starters = proj_starters.drop(columns=["resolved_game_id_norm"])

    # Prepare starters full context unique on (player_id_norm, game_id_norm)
    sp_ctx = (
        sp.loc[:, ["player_id_norm", "game_id_norm", "team_id", "opponent_team_id"]]
          .drop_duplicates()
          .copy()
    )

    # Resolve any context duplicates by deterministic sort and keep first
    sp_ctx["_gid_num"] = pd.to_numeric(sp_ctx["game_id_norm"], errors="coerce")
    sp_ctx = (
        sp_ctx.sort_values(["player_id_norm", "_gid_num", "game_id_norm", "team_id", "opponent_team_id"])
              .drop_duplicates(subset=["player_id_norm", "game_id_norm"], keep="first")
              .drop(columns=["_gid_num"])
    )

    # Merge full context by normalized keys
    merged = proj_starters.merge(
        sp_ctx,
        on=["player_id_norm", "game_id_norm"],
        how="left",
        suffixes=("", "_ctx")
    )

    # Validate context present; log only
    missing_ctx_mask = merged["team_id"].isna() | merged["opponent_team_id"].isna()
    if missing_ctx_mask.any():
        bad = merged.loc[missing_ctx_mask, ["player_id_norm", "game_id_norm", "team_id", "opponent_team_id"]].drop_duplicates()
        write_csv(bad, SUM_DIR / "missing_pitcher_context_after_merge.csv")

    # Construct final frame:
    # - Start from original projection columns
    # - Replace/add canonical 'game_id', 'team_id', 'opponent_team_id' (strings)
    out = merged.copy()

    # Ensure plain string dtype for output keys
    out["game_id"] = out["game_id_norm"].astype("string")
    out["team_id"] = out["team_id"].astype("string")
    out["opponent_team_id"] = out["opponent_team_id"].astype("string")

    # Drop helper columns
    drop_cols = [c for c in out.columns if c.endswith(("_x", "_y", "_sp", "_ctx"))]
    drop_cols += ["player_id_norm", "game_id_norm"]
    out = out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")

    # Reorder: keep original projection columns first (if still present), then keys
    proj_cols_order = [c for c in proj.columns if c in out.columns]
    tail_cols = [c for c in ["game_id", "team_id", "opponent_team_id"] if c not in proj_cols_order]
    out = out[proj_cols_order + tail_cols]

    # Write outputs and diagnostics
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(out, OUT_FILE)

    # Summary
    summary_lines = [
        f"rows_proj_total={len(proj)}",
        f"rows_starters_input={len(sp)}",
        f"rows_out_final={len(out)}",
        f"proj_missing_in_starters={len(missing_in_sp)}",
        f"dup_players_logged={'yes' if not dup_players.empty else 'no'}",
    ]
    write_text(SUM_DIR / "status_project_pitchers.txt", "OK project_pitcher_props.py | " + " ".join(summary_lines))
    print(f"[OK] WROTE {OUT_FILE} rows={len(out)} | " + " ".join(summary_lines))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        write_text(SUM_DIR / "status_project_pitchers.txt", "FAIL project_pitcher_props.py")
        write_text(SUM_DIR / "errors_project_pitchers.txt", repr(e))
        print(e)
        sys.exit(1)
