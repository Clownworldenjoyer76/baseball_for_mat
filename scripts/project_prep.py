#!/usr/bin/env python3
# Robustly enrich starting pitchers with slate context (game_id, team_id, opponent_team_id).
# - Input (authoritative slate): data/raw/todaysgames_normalized.csv
#     required: game_id, home_team_id, away_team_id, pitcher_home_id, pitcher_away_id
# - Input (to enrich): data/raw/startingpitchers_with_opp_context.csv
#     must have at least one pitcher id column: pitcher_id OR player_id OR mlb_id OR id
# - Output (overwrite in place): data/raw/startingpitchers_with_opp_context.csv (now has game_id, team_id, opponent_team_id, side)
# - Output (minimal mirror for downstream): data/end_chain/final/startingpitchers.csv
#
# Safety:
# - All columns read as strings; blank/None/nan normalized to "".
# - No KeyError/IndexError if a column is absent; we coalesce safely.
# - Unmatched rows are kept, with empty game/team fields, and counted in logs.

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
        df[c] = df[c].astype(str).str.strip().replace({"None": "", "none": "", "nan": "", "NaN": ""})
    return df

def coalesce(df: pd.DataFrame, *cols: str, default: str = "") -> pd.Series:
    """Return first non-empty string among the given columns."""
    if not cols:
        return pd.Series([default]*len(df), index=df.index, dtype="object")
    out = pd.Series([default]*len(df), index=df.index, dtype="object")
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str).fillna("")
            out = out.where(out.astype(str).str.len() > 0, s)
    return out.fillna(default).astype(str)

def normalize_id(s: str) -> str:
    s = str(s or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s

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

    # Normalize a canonical pitcher_id in SP
    # We accept any of these columns and coalesce into 'pitcher_id'
    sp["pitcher_id"] = coalesce(sp, "pitcher_id", "player_id", "mlb_id", "id").map(normalize_id)

    # For downstream compatibility, ensure a 'player_id' column exists and mirrors pitcher_id
    if "player_id" not in sp.columns:
        sp["player_id"] = ""
    sp["player_id"] = sp["player_id"].map(normalize_id)
    sp["player_id"] = sp["player_id"].where(sp["player_id"].str.len() > 0, sp["pitcher_id"])

    # Ensure presence of target columns to avoid index errors later
    for col in ["game_id", "team_id", "opponent_team_id", "side"]:
        if col not in sp.columns:
            sp[col] = ""

    # Prepare slim slate frames for join (home & away), keeping only what we need
    tgn_slim = tgn[["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]].copy()
    for c in tgn_slim.columns:
        tgn_slim[c] = tgn_slim[c].map(normalize_id)

    # Home match
    home_join = tgn_slim.rename(columns={"pitcher_home_id": "pitcher_id"}).copy()
    home_join["side"] = "home"
    home_join["team_id"] = tgn_slim["home_team_id"]
    home_join["opponent_team_id"] = tgn_slim["away_team_id"]
    home_join = home_join[["pitcher_id", "game_id", "team_id", "opponent_team_id", "side"]]

    # Away match
    away_join = tgn_slim.rename(columns={"pitcher_away_id": "pitcher_id"}).copy()
    away_join["side"] = "away"
    away_join["team_id"] = tgn_slim["away_team_id"]
    away_join["opponent_team_id"] = tgn_slim["home_team_id"]
    away_join = away_join[["pitcher_id", "game_id", "team_id", "opponent_team_id", "side"]]

    # Combine home/away mapping; drop blanks and dups
    map_df = pd.concat([home_join, away_join], ignore_index=True)
    for c in ["pitcher_id", "game_id", "team_id", "opponent_team_id", "side"]:
        map_df[c] = map_df[c].astype(str)
    map_df = map_df[map_df["pitcher_id"].str.len() > 0].drop_duplicates()

    # Left-join SP to mapping on pitcher_id
    merged = sp.merge(map_df, on="pitcher_id", how="left", suffixes=("", "_from_tgn"))

    # Coalesce into final columns (prefer pre-existing if non-empty, otherwise from_tgn)
    for c in ["game_id", "team_id", "opponent_team_id", "side"]:
        existing = merged[c] if c in merged.columns else pd.Series([""] * len(merged))
        incoming = merged[f"{c}_from_tgn"] if f"{c}_from_tgn" in merged.columns else pd.Series([""] * len(merged))
        merged[c] = existing.where(existing.astype(str).str.len() > 0, incoming.astype(str))
        if f"{c}_from_tgn" in merged.columns:
            merged.drop(columns=[f"{c}_from_tgn"], inplace=True)

    # Normalize ID-like columns to plain strings (no NaN/.0)
    for c in ["player_id", "pitcher_id", "game_id", "team_id", "opponent_team_id"]:
        merged[c] = merged[c].map(normalize_id)

    # Log coverage
    total = len(merged)
    matched = int((merged["game_id"].astype(str).str.len() > 0).sum())
    unmatched = total - matched
    both_teams = int(((merged["team_id"].astype(str).str.len() > 0) &
                      (merged["opponent_team_id"].astype(str).str.len() > 0)).sum())

    print(f"[COVERAGE] rows_in={total}, matched_game_id={matched}, unmatched={unmatched}, "
          f"with_team_and_opponent={both_teams}", flush=True)

    # Persist outputs
    # 1) Overwrite the source raw file (keeps all original columns + new fields)
    merged.to_csv(OUT_RAW, index=False)
    print(f"✅ Wrote enriched raw: {OUT_RAW} (rows={len(merged)})", flush=True)

    # 2) Minimal “final” mirror for downstream references (keep common fields)
    keep_cols = [c for c in ["pitcher_id", "player_id", "name", "team", "game_id", "team_id", "opponent_team_id", "side"] if c in merged.columns]
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
