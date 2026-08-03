#!/usr/bin/env python3
# Build/normalize pitcher projections for 06 so downstream steps always have files.
#
# Behavior:
# - Primary source (if present): data/_projections/pitcher_props_projected.csv
# - Fallback build: derive a minimal projection frame from
#       data/raw/startingpitchers_with_opp_context.csv
#   with core columns: player_id, pitcher_id, game_id, team_id, opponent_team_id, side, name
#   and placeholder numeric columns (left blank) for schema enforcers to populate/validate.
# - Always write BOTH:
#       data/_projections/pitcher_props_projected.csv
#       data/end_chain/final/pitcher_props_projected_final.csv
#
# Notes:
# - All I/O as strings; ID-like fields normalized to plain strings (no NaN/.0).
# - Never fail if we can produce a sane minimal file; raise only on truly missing SP file.

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

PROJ_DIR   = Path("data/_projections")
RAW_DIR    = Path("data/raw")
FINAL_DIR  = Path("data/end_chain/final")

SRC_PROJ   = PROJ_DIR / "pitcher_props_projected.csv"          # preferred source
OUT_PROJ   = PROJ_DIR / "pitcher_props_projected.csv"          # ensure we (re)write this
OUT_FINAL  = FINAL_DIR / "pitcher_props_projected_final.csv"   # and the mirrored final
SP_LONG    = RAW_DIR / "startingpitchers_with_opp_context.csv"

# Minimal numeric/score columns expected later; left blank here and enforced by ensure_* script.
PLACEHOLDER_NUMERIC_COLS = [
    "outs_proj", "ip_proj", "k_proj", "bb_proj", "er_proj",
    "hits_proj", "hr_proj", "bf_proj", "whip_proj", "era_proj",
]

def read_csv_str(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().replace({"None":"","none":"","nan":"","NaN":""})
    return df

def normalize_id(s: str) -> str:
    s = str(s or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s

def coalesce(df: pd.DataFrame, *cols: str, default: str = "") -> pd.Series:
    if not cols:
        return pd.Series([default]*len(df), index=df.index, dtype="object")
    out = pd.Series([default]*len(df), index=df.index, dtype="object")
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str).fillna("")
            out = out.where(out.astype(str).str.len() > 0, s)
    return out.fillna(default).astype(str)

def build_from_starting_pitchers() -> pd.DataFrame:
    if not SP_LONG.exists():
        raise FileNotFoundError(f"{SP_LONG} not found (needed to seed pitcher projections).")
    sp = read_csv_str(SP_LONG)

    # Ensure core id/context columns exist
    sp["pitcher_id"] = coalesce(sp, "pitcher_id", "player_id", "mlb_id", "id")
    if "player_id" not in sp.columns:
        sp["player_id"] = ""
    sp["player_id"] = sp["player_id"].where(sp["player_id"].str.len() > 0, sp["pitcher_id"])

    for c in ["player_id","pitcher_id","game_id","team_id","opponent_team_id","side","name","team"]:
        if c not in sp.columns:
            sp[c] = ""

    # Normalize IDs
    for c in ["player_id","pitcher_id","game_id","team_id","opponent_team_id"]:
        sp[c] = sp[c].map(normalize_id)

    # Drop rows without a pitcher_id
    sp = sp[sp["pitcher_id"].str.len() > 0].copy()

    # Build the minimal projection frame
    cols = ["player_id","pitcher_id","name","team","game_id","team_id","opponent_team_id","side"]
    proj = sp[cols].copy()

    # Add placeholder numeric columns (blank)
    for c in PLACEHOLDER_NUMERIC_COLS:
        proj[c] = ""

    # Ensure uniqueness on (player_id, game_id) to avoid dup downstream joins
    if {"player_id","game_id"}.issubset(proj.columns):
        proj = (proj.sort_values(["player_id","game_id"])
                    .drop_duplicates(subset=["player_id","game_id"], keep="first"))

    return proj

def main() -> None:
    print(">> START: project_pitcher_props.py v14-seed-from-sp-long-if-missing", flush=True)
    print(f"[PATH] PROJ_SRC_IN={SRC_PROJ.resolve()}", flush=True)
    print(f"[PATH] SP_LONG={SP_LONG.resolve()}", flush=True)
    print(f"[PATH] OUT_FILE_FINAL={OUT_FINAL.resolve()}", flush=True)
    print(f"[PATH] OUT_FILE_PROJ={OUT_PROJ.resolve()}", flush=True)

    # If a projection already exists, read/normalize it; otherwise build from SP long.
    if SRC_PROJ.exists():
        print(f"[INFO] Using existing projection source: {SRC_PROJ}", flush=True)
        proj = read_csv_str(SRC_PROJ)
        # Minimal normalization of IDs
        for c in ["player_id","pitcher_id","game_id","team_id","opponent_team_id"]:
            if c in proj.columns:
                proj[c] = proj[c].map(normalize_id)
        # Ensure presence of core columns
        for c in ["player_id","pitcher_id","game_id","team_id","opponent_team_id","side","name","team"]:
            if c not in proj.columns:
                proj[c] = ""
        # Add any missing placeholder columns
        for c in PLACEHOLDER_NUMERIC_COLS:
            if c not in proj.columns:
                proj[c] = ""
    else:
        print(f"[INFO] {SRC_PROJ} not found; seeding projections from {SP_LONG.name}", flush=True)
        proj = build_from_starting_pitchers()

    # Final write: always produce both files
    PROJ_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    proj.to_csv(OUT_PROJ, index=False)
    proj.to_csv(OUT_FINAL, index=False)

    print(f"✅ Wrote {OUT_PROJ} (rows={len(proj)})", flush=True)
    print(f"✅ Wrote {OUT_FINAL} (rows={len(proj)})", flush=True)
    print(">> END: project_pitcher_props.py", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(repr(e), file=sys.stderr, flush=True)
        raise
