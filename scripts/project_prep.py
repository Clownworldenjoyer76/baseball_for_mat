#!/usr/bin/env python3
# Robustly enrich starting pitchers with slate context (game_id, team_id, opponent_team_id).
# - Input (authoritative slate): data/raw/todaysgames_normalized.csv
#     required: game_id, home_team_id, away_team_id, pitcher_home_id, pitcher_away_id
# - Input (to enrich): data/raw/startingpitchers_with_opp_context.csv
#     must have at least one pitcher id column: pitcher_id OR player_id
# - Output (overwrite in place): data/raw/startingpitchers_with_opp_context.csv (now has game_id, team_id, opponent_team_id)
# - Output (minimal mirror for downstream): data/end_chain/final/startingpitchers.csv
#
# Safety:
# - All columns read as strings; blank/None/nan normalized to "".
# - No KeyError/IndexError if a column is absent; we coalesce safely.
# - If upstream starters file has no usable pitcher IDs, we fall back to the slate so
#   downstream steps still have complete coverage by game/side/team.

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

RAW_DIR   = Path("data/raw")
FINAL_DIR = Path("data/end_chain/final")
PROJ_DIR  = Path("data/_projections")
FINAL_DIR.mkdir(parents=True, exist_ok=True)
PROJ_DIR.mkdir(parents=True, exist_ok=True)

# Files
SP_LONG = RAW_DIR / "startingpitchers_with_opp_context.csv"
TGN_CSV = RAW_DIR / "todaysgames_normalized.csv"

# For historical compatibility with logs in your workflow
OUT_RAW   = SP_LONG
OUT_FINAL = FINAL_DIR / "startingpitchers.csv"
OUT_PROJ  = PROJ_DIR / "pitcher_props_projected.csv"   # not required to write here; we just log the path

def read_csv_str(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().replace({"None": "", "nan": "", "NaN": ""})
    return df

def normalize_id_str(val: str) -> str:
    """Clean an ID-like string: strip, remove trailing '.0', drop NaN-like."""
    s = str(val or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s

def norm_ids(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(normalize_id_str)
    return df

def coalesce(df: pd.DataFrame, *cols: str, default: str = "") -> pd.Series:
    if not cols:
        return pd.Series([default]*len(df), index=df.index, dtype="object")
    out = pd.Series([default]*len(df), index=df.index, dtype="object")
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str)
            out = out.where(out.astype(str).str.len() > 0, s)
    return out.fillna(default).astype(str)

def build_map_from_tgn(tgn: pd.DataFrame) -> pd.DataFrame:
    """Return long mapping of pitcher_id -> (game_id, team_id, opponent_team_id, side)."""
    need = ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]
    missing = [c for c in need if c not in tgn.columns]
    if missing:
        raise RuntimeError(f"{TGN_CSV} missing required column(s): {missing}")

    # normalize ID-ish columns to clean strings
    tgn = tgn.copy()
    tgn = norm_ids(tgn, ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"])

    # home rows
    home = tgn.rename(columns={"pitcher_home_id": "pitcher_id"}).copy()
    home["side"] = "home"
    home["team_id"] = home["home_team_id"]
    home["opponent_team_id"] = home["away_team_id"]
    home = home[["pitcher_id", "game_id", "team_id", "opponent_team_id", "side"]]

    # away rows
    away = tgn.rename(columns={"pitcher_away_id": "pitcher_id"}).copy()
    away["side"] = "away"
    away["team_id"] = away["away_team_id"]
    away["opponent_team_id"] = away["home_team_id"]
    away = away[["pitcher_id", "game_id", "team_id", "opponent_team_id", "side"]]

    long_map = pd.concat([home, away], ignore_index=True)
    for c in ["pitcher_id", "game_id", "team_id", "opponent_team_id", "side"]:
        if c in long_map.columns:
            long_map[c] = long_map[c].astype(str).str.strip()
    # keep duplicates out, but keep blanks (so we preserve rows even if a pitcher_id is missing on slate)
    long_map = long_map.drop_duplicates()
    return long_map

def build_fallback_from_tgn(tgn: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a minimal starters list directly from the slate (two rows per game),
    carrying name/team if present, and full context fields.
    """
    tgn = tgn.copy()
    # Normalize IDs
    tgn = norm_ids(tgn, ["game_id", "home_team_id", "away_team_id",
                         "pitcher_home_id", "pitcher_away_id"])
    # Optional name/team columns
    # If absent, they will just come through as empty strings.
    for col in ["pitcher_home", "pitcher_away", "home_team", "away_team"]:
        if col not in tgn.columns:
            tgn[col] = ""

    # Build home/away rows
    home_rows = tgn[["game_id", "home_team_id", "away_team_id",
                     "pitcher_home_id", "pitcher_home", "home_team"]].copy()
    home_rows.rename(columns={
        "home_team_id": "team_id",
        "away_team_id": "opponent_team_id",
        "pitcher_home_id": "pitcher_id",
        "pitcher_home": "name",
        "home_team": "team",
    }, inplace=True)
    home_rows["side"] = "home"

    away_rows = tgn[["game_id", "away_team_id", "home_team_id",
                     "pitcher_away_id", "pitcher_away", "away_team"]].copy()
    away_rows.rename(columns={
        "away_team_id": "team_id",
        "home_team_id": "opponent_team_id",
        "pitcher_away_id": "pitcher_id",
        "pitcher_away": "name",
        "away_team": "team",
    }, inplace=True)
    away_rows["side"] = "away"

    out = pd.concat([home_rows, away_rows], ignore_index=True)
    # Final column order / cleanup
    for c in ["pitcher_id", "game_id", "team_id", "opponent_team_id", "side", "name", "team"]:
        if c not in out.columns:
            out[c] = ""
    out = out[["pitcher_id", "name", "team", "game_id", "team_id", "opponent_team_id", "side"]]
    # Clean again for safety
    out = norm_ids(out, ["pitcher_id", "game_id", "team_id", "opponent_team_id"])
    return out.drop_duplicates().reset_index(drop=True)

def main() -> None:
    print(">> START: project_prep.py", flush=True)
    print(f"[PATH] OUT_RAW={OUT_RAW.resolve()}", flush=True)
    print(f"[PATH] OUT_FINAL={OUT_FINAL.resolve()}", flush=True)
    print(f"[PATH] OUT_PROJ={OUT_PROJ.resolve()}", flush=True)

    # Read inputs
    tgn = read_csv_str(TGN_CSV)
    sp  = read_csv_str(SP_LONG)

    # Validate TGN minimal schema
    need_tgn = ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]
    missing_tgn = [c for c in need_tgn if c not in tgn.columns]
    if missing_tgn:
        raise RuntimeError(f"{TGN_CSV} missing required column(s): {missing_tgn}")

    # Normalize a canonical pitcher_id in SP (coalesce many possible columns)
    sp["pitcher_id"] = coalesce(sp, "pitcher_id", "player_id", "mlb_id", "id").astype(str)
    sp["pitcher_id"] = sp["pitcher_id"].apply(normalize_id_str)

    # Ensure presence of target columns to avoid index errors later
    for col in ["game_id", "team_id", "opponent_team_id", "side", "name", "team"]:
        if col not in sp.columns:
            sp[col] = ""

    # Build mapping from slate
    map_df = build_map_from_tgn(tgn)

    # Decide if we must fallback
    no_ids_in_sp = (sp["pitcher_id"].str.len() == 0).all()
    use_fallback = False
    merged = None

    if not no_ids_in_sp:
        # Left-join SP to mapping on pitcher_id
        merged = sp.merge(map_df, on="pitcher_id", how="left", suffixes=("", "_from_tgn"))

        # Coalesce into final columns (prefer pre-existing if non-empty, otherwise from_tgn)
        for c in ["game_id", "team_id", "opponent_team_id", "side"]:
            existing = merged[c] if c in merged.columns else pd.Series([""] * len(merged))
            incoming = merged.get(f"{c}_from_tgn", pd.Series([""] * len(merged)))
            merged[c] = existing.where(existing.astype(str).str.len() > 0, incoming.astype(str))
            drop_col = f"{c}_from_tgn"
            if drop_col in merged.columns:
                merged.drop(columns=[drop_col], inplace=True)

        # Clean ID-ish columns
        merged = norm_ids(merged, ["pitcher_id", "game_id", "team_id", "opponent_team_id"])

        # Coverage
        matched = int((merged["game_id"].astype(str).str.len() > 0).sum())
        total = len(merged)
        if matched == 0:
            use_fallback = True
            print("[WARN] project_prep: 0 rows matched via starters file; using slate fallback.", flush=True)
        else:
            unmatched = total - matched
            both_teams = int(((merged["team_id"].astype(str).str.len() > 0) &
                              (merged["opponent_team_id"].astype(str).str.len() > 0)).sum())
            print(f"[COVERAGE] rows_in={total}, matched_game_id={matched}, unmatched={unmatched}, "
                  f"with_team_and_opponent={both_teams}", flush=True)
    else:
        use_fallback = True
        print("[WARN] project_prep: starters file has no non-empty pitcher_id; using slate fallback.", flush=True)

    if use_fallback:
        # Build directly from the slate
        merged = build_fallback_from_tgn(tgn)
        # Overwrite the raw starters file with this minimal-but-complete context
        # (preserve original columns if you prefer—here we provide the essential ones)
        print(f"[INFO] project_prep: slate fallback produced {len(merged)} rows.", flush=True)

    # Persist outputs
    merged.to_csv(OUT_RAW, index=False)
    print(f"✅ Wrote enriched raw: {OUT_RAW} (rows={len(merged)})", flush=True)

    keep_cols = [c for c in ["pitcher_id", "name", "team", "game_id", "team_id", "opponent_team_id", "side"] if c in merged.columns]
    merged[keep_cols].to_csv(OUT_FINAL, index=False)
    print(f"✅ Wrote minimal final: {OUT_FINAL} (rows={len(merged)})", flush=True)

    print(">> END: project_prep.py", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Ensure a clear failure message in the 06 summary log
        print(repr(e), file=sys.stderr, flush=True)
        raise
