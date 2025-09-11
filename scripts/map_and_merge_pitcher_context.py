#!/usr/bin/env python3
# Stage: 06
# Goal: Produce data/end_chain/final/pitcher_props_projected_final.csv by aligning ID namespaces via an explicit crosswalk.
# Inputs (required):
#   - data/_projections/pitcher_props_projected.csv              [must have: player_id]
#   - data/raw/startingpitchers_with_opp_context.csv             [must have: player_id, game_id, team_id, opponent_team_id]
#   - data/raw/pitcher_id_crosswalk.csv                           [must have: proj_player_id, sp_player_id]
# Output:
#   - data/end_chain/final/pitcher_props_projected_final.csv
# Diagnostics:
#   - summaries/06_pitcher_props/*.csv|.txt

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

PROJ_IN   = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SP_LONG   = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
XWALK_IN  = ROOT / "data" / "raw" / "pitcher_id_crosswalk.csv"
OUT_DIR   = ROOT / "data" / "end_chain" / "final"
OUT_FILE  = OUT_DIR / "pitcher_props_projected_final.csv"
SUM_DIR   = ROOT / "summaries" / "06_pitcher_props"

REQ_PROJ = ["player_id"]
REQ_SP   = ["player_id","game_id","team_id","opponent_team_id"]
REQ_XW   = ["proj_player_id","sp_player_id"]

def must_have(df: pd.DataFrame, cols: list[str], name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing columns: {miss}")

def to_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df

def write_csv(df: pd.DataFrame, p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)

def write_text(p: Path, s: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8")

def main() -> int:
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    print(f">> START map_and_merge_pitcher_context.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")

    if not PROJ_IN.exists() or not SP_LONG.exists() or not XWALK_IN.exists():
        missing = [p for p in [PROJ_IN, SP_LONG, XWALK_IN] if not p.exists()]
        raise FileNotFoundError(f"Missing inputs: {', '.join(str(x) for x in missing)}")

    proj = pd.read_csv(PROJ_IN, dtype=str).fillna("")
    sp   = pd.read_csv(SP_LONG, dtype=str).fillna("")
    xw   = pd.read_csv(XWALK_IN, dtype=str).fillna("")

    must_have(proj, REQ_PROJ,  str(PROJ_IN))
    must_have(sp,   REQ_SP,    str(SP_LONG))
    must_have(xw,   REQ_XW,    str(XWALK_IN))

    proj = to_str(proj, ["player_id","game_id"])
    sp   = to_str(sp,   ["player_id","game_id","team_id","opponent_team_id"])
    xw   = to_str(xw,   ["proj_player_id","sp_player_id"])

    # Normalize IDs as integer-like strings where possible
    def norm(s: str) -> str:
        try:
            return str(int(float(s)))
        except Exception:
            return s.strip()

    proj["player_id_norm"] = proj["player_id"].map(norm)
    sp["player_id_norm"]   = sp["player_id"].map(norm)
    xw["proj_player_id_norm"] = xw["proj_player_id"].map(norm)
    xw["sp_player_id_norm"]   = xw["sp_player_id"].map(norm)

    # Crosswalk sanity
    dup_xw = xw.groupby("proj_player_id_norm", as_index=False)["sp_player_id_norm"].nunique().query("sp_player_id_norm > 1")
    if not dup_xw.empty:
        write_csv(dup_xw, SUM_DIR / "crosswalk_ambiguous_proj_ids.csv")
        raise RuntimeError("Crosswalk ambiguous: one proj_player_id maps to multiple sp_player_id; see summaries/06_pitcher_props/crosswalk_ambiguous_proj_ids.csv")

    # Map projections -> starters namespace
    proj_mapped = proj.merge(
        xw[["proj_player_id_norm","sp_player_id_norm"]],
        left_on="player_id_norm",
        right_on="proj_player_id_norm",
        how="left",
        suffixes=("", "_xw")
    )
    missing_map = proj_mapped[proj_mapped["sp_player_id_norm"].isna() | (proj_mapped["sp_player_id_norm"]=="")]
    if not missing_map.empty:
        write_csv(missing_map[["player_id"]].drop_duplicates(), SUM_DIR / "unmapped_projection_player_ids.csv")
        raise RuntimeError("Unmapped projection player_id; see summaries/06_pitcher_props/unmapped_projection_player_ids.csv")

    # Build starters key map (unique sp_player_id_norm -> game_id)
    sp_keys = (
        sp[["player_id_norm","game_id"]]
        .query("game_id != '' and game_id != 'UNKNOWN'")
        .drop_duplicates()
        .rename(columns={"player_id_norm":"sp_player_id_norm"})
    )

    # Resolve any duplicates per sp_player_id_norm deterministically (lowest numeric game_id)
    sp_keys["_gid_num"] = pd.to_numeric(sp_keys["game_id"], errors="coerce")
    sp_keys = (
        sp_keys.sort_values(["sp_player_id_norm","_gid_num","game_id"])
               .drop(columns=["_gid_num"])
               .drop_duplicates(subset=["sp_player_id_norm"], keep="first")
    )

    # Inject game_id into projections (now in starters namespace via crosswalk)
    proj_inj = proj_mapped.merge(sp_keys, on="sp_player_id_norm", how="left", suffixes=("", "_from_sp"))
    missing_gid = proj_inj[proj_inj["game_id"].isna() | (proj_inj["game_id"]=="")]
    if not missing_gid.empty:
        write_csv(missing_gid[["player_id","sp_player_id_norm"]].drop_duplicates(), SUM_DIR / "missing_gameid_after_injection.csv")
        raise RuntimeError("Failed to inject game_id; see summaries/06_pitcher_props/missing_gameid_after_injection.csv")

    proj_inj["game_id"] = proj_inj["game_id"].astype("string")

    # Merge full context on (sp_player_id_norm, game_id)
    sp_ctx = sp[["player_id_norm","game_id","team_id","opponent_team_id"]].drop_duplicates()
    merged = proj_inj.merge(
        sp_ctx.rename(columns={"player_id_norm":"sp_player_id_norm"}),
        on=["sp_player_id_norm","game_id"],
        how="left"
    )

    need = ["team_id","opponent_team_id"]
    if merged[need].isna().any().any() or (merged[need] == "").any().any():
        bad = merged.loc[merged[need].isna().any(axis=1) | (merged[need] == "").any(axis=1), ["player_id","game_id"] + need].drop_duplicates()
        write_csv(bad, SUM_DIR / "missing_context_after_merge.csv")
        raise RuntimeError("Missing team_id/opponent_team_id; see summaries/06_pitcher_props/missing_context_after_merge.csv")

    # Clean artifacts
    drop_cols = [c for c in ["player_id_norm","proj_player_id_norm","sp_player_id_norm"] if c in merged.columns]
    merged = merged.drop(columns=drop_cols, errors="ignore")
    merged = merged.drop(columns=[c for c in merged.columns if c.endswith(("_x","_y","_sp","_ctx","_xw"))], errors="ignore")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)

    write_text(SUM_DIR / "status_map_and_merge.txt", f"OK map_and_merge rows={len(merged)}")
    print(f"[OK] WROTE {OUT_FILE} rows={len(merged)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        write_text(SUM_DIR / "status_map_and_merge.txt", "FAIL map_and_merge")
        write_text(SUM_DIR / "errors_map_and_merge.txt", repr(e))
        print(e)
        sys.exit(1)
