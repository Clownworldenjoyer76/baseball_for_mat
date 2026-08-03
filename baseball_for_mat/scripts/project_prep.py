#!/usr/bin/env python3
# Robustly enrich starting pitchers with slate context (game_id, team_id, opponent_team_id, side).
# Inputs (authoritative slate): data/raw/todaysgames_normalized.csv
#   required: game_id, home_team_id, away_team_id, pitcher_home_id, pitcher_away_id
# Input (to enrich): data/raw/startingpitchers_with_opp_context.csv
#   must have at least one pitcher id column: pitcher_id OR player_id (we also accept mlb_id, id)
# Outputs:
#   - overwrite in place: data/raw/startingpitchers_with_opp_context.csv (adds game_id, team_id, opponent_team_id, side)
#   - minimal mirror:     data/end_chain/final/startingpitchers.csv
#   - diagnostics:        summaries/06_projection/project_prep_unmatched_starting_pitchers.csv
#
# Safety:
# - All columns read as strings; "nan"/"None" cleaned to "".
# - Never produce float-like IDs (no ".0"); normalize to plain strings.
# - Keep all original columns; add/overwrite only the target enrichment fields.

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

RAW_DIR   = Path("data/raw")
FINAL_DIR = Path("data/end_chain/final")
PROJ_DIR  = Path("data/_projections")
SUM_DIR   = Path("summaries/06_projection")

FINAL_DIR.mkdir(parents=True, exist_ok=True)
PROJ_DIR.mkdir(parents=True, exist_ok=True)
SUM_DIR.mkdir(parents=True, exist_ok=True)

# Files
SP_LONG  = RAW_DIR / "startingpitchers_with_opp_context.csv"
TGN_CSV  = RAW_DIR / "todaysgames_normalized.csv"

# For historical compatibility with logs in your workflow
OUT_RAW    = SP_LONG
OUT_FINAL  = FINAL_DIR / "startingpitchers.csv"
UNMATCHED  = SUM_DIR / "project_prep_unmatched_starting_pitchers.csv"

# ---------- utils ----------

def read_csv_str(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = (
            df[c]
            .astype(str)
            .str.strip()
            .replace({"None": "", "none": "", "NaN": "", "nan": ""})
        )
    return df

def normalize_id(val: str) -> str:
    """Force IDs to plain, digit-only strings when possible, drop trailing '.0', never 'nan'/'None'."""
    s = str(val or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    # Common offender: numeric read as float-like '123.0'
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s

def coalesce_cols(df: pd.DataFrame, targets: list[str], out_col: str) -> pd.DataFrame:
    """Coalesce the first non-empty value across 'targets' into 'out_col'."""
    if out_col not in df.columns:
        df[out_col] = ""
    # Start with existing out_col if present
    s = df[out_col].astype(str).apply(normalize_id) if out_col in df.columns else pd.Series([""]*len(df))
    for col in targets:
        if col in df.columns:
            cand = df[col].astype(str).apply(normalize_id)
            s = s.where(s.str.len() > 0, cand)
    df[out_col] = s.fillna("").astype(str)
    return df

def build_tgn_map(tgn: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a tidy map:
      columns: pitcher_id, game_id, team_id, opponent_team_id, side
      two rows per game (home/away), only where pitcher_id is non-empty
    """
    need = ["game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"]
    miss = [c for c in need if c not in tgn.columns]
    if miss:
        raise RuntimeError(f"{TGN_CSV} missing required column(s): {miss}")

    for c in need:
        tgn[c] = tgn[c].apply(normalize_id)

    # Home side
    home = pd.DataFrame({
        "pitcher_id":       tgn["pitcher_home_id"].astype(str).apply(normalize_id),
        "game_id":          tgn["game_id"].astype(str).apply(normalize_id),
        "team_id":          tgn["home_team_id"].astype(str).apply(normalize_id),
        "opponent_team_id": tgn["away_team_id"].astype(str).apply(normalize_id),
        "side":             "home",
    })

    # Away side
    away = pd.DataFrame({
        "pitcher_id":       tgn["pitcher_away_id"].astype(str).apply(normalize_id),
        "game_id":          tgn["game_id"].astype(str).apply(normalize_id),
        "team_id":          tgn["away_team_id"].astype(str).apply(normalize_id),
        "opponent_team_id": tgn["home_team_id"].astype(str).apply(normalize_id),
        "side":             "away",
    })

    map_df = pd.concat([home, away], ignore_index=True)
    # keep only real pitcher ids
    map_df = map_df[map_df["pitcher_id"].str.len() > 0].drop_duplicates()
    return map_df

def write_minimal_final(df: pd.DataFrame, path: Path) -> None:
    keep_cols = [c for c in ["pitcher_id", "player_id", "name", "team", "game_id", "team_id", "opponent_team_id", "side"] if c in df.columns]
    df[keep_cols].to_csv(path, index=False)

# ---------- main ----------

def main() -> None:
    print(">> START: project_prep.py", flush=True)
    print(f"[PATH] OUT_RAW={OUT_RAW.resolve()}", flush=True)
    print(f"[PATH] OUT_FINAL={OUT_FINAL.resolve()}", flush=True)

    # Read inputs
    tgn = read_csv_str(TGN_CSV)
    sp  = read_csv_str(SP_LONG)

    # Canonical pitcher_id on SP (coalesce across common id columns)
    # Order matters: honor existing 'pitcher_id' if present; else fallback to player_id -> mlb_id -> id
    sp = coalesce_cols(sp, ["pitcher_id", "player_id", "mlb_id", "id"], out_col="pitcher_id")

    # Ensure a 'player_id' column exists (downstream guard expects it). If missing/blank, mirror pitcher_id.
    if "player_id" not in sp.columns:
        sp["player_id"] = ""
    sp.loc[sp["player_id"].astype(str).str.len() == 0, "player_id"] = sp["pitcher_id"].astype(str)

    # Make sure enrichment columns exist (safe overwrite later)
    for col in ["game_id", "team_id", "opponent_team_id", "side"]:
        if col not in sp.columns:
            sp[col] = ""
        else:
            sp[col] = sp[col].astype(str).apply(normalize_id)

    # Build slate mapping from TGN
    map_df = build_tgn_map(tgn)

    # Merge by pitcher_id (string join)
    for c in ["pitcher_id"]:
        sp[c] = sp[c].astype(str).apply(normalize_id)
    merged = sp.merge(map_df, on="pitcher_id", how="left", suffixes=("", "_from_tgn"))

    # Coalesce enriched fields (prefer existing non-empty; else from_tgn)
    for c in ["game_id", "team_id", "opponent_team_id", "side"]:
        existing = merged[c] if c in merged.columns else pd.Series([""] * len(merged))
        incoming = merged[f"{c}_from_tgn"] if f"{c}_from_tgn" in merged.columns else pd.Series([""] * len(merged))
        merged[c] = existing.astype(str)
        merged[c] = merged[c].where(merged[c].str.len() > 0, incoming.astype(str))
        merged[c] = merged[c].apply(normalize_id)
        if f"{c}_from_tgn" in merged.columns:
            merged.drop(columns=[f"{c}_from_tgn"], inplace=True)

    # Diagnostics: unmatched rows (no game_id after merge)
    unmatched_mask = merged["game_id"].astype(str).str.len() == 0
    unmatched_df = merged.loc[unmatched_mask, [c for c in ["pitcher_id", "player_id", "name", "team"] if c in merged.columns]].drop_duplicates()
    if not unmatched_df.empty:
        unmatched_df.to_csv(UNMATCHED, index=False)
        print(f"[WARN] Unmatched pitchers (no game_id): {len(unmatched_df)} -> {UNMATCHED}", flush=True)
    else:
        # If file exists from prior runs and now empty, keep history; don't delete.
        print("[OK] All pitchers matched to a game_id.", flush=True)

    # Sort for readability: by game_id, then side (home before away)
    side_order = {"home": 0, "away": 1}
    if "side" in merged.columns:
        merged["_side_rank"] = merged["side"].map(side_order).fillna(9).astype(int)
    else:
        merged["_side_rank"] = 9
    merged["_gid_num"] = pd.to_numeric(merged["game_id"], errors="coerce")

    merged = merged.sort_values(by=["_gid_num", "_side_rank", "pitcher_id"], kind="stable").drop(columns=["_gid_num", "_side_rank"])

    # Persist outputs
    merged.to_csv(OUT_RAW, index=False)
    print(f"✅ Wrote enriched raw: {OUT_RAW} (rows={len(merged)})", flush=True)

    write_minimal_final(merged, OUT_FINAL)
    print(f"✅ Wrote minimal final: {OUT_FINAL} (rows={len(merged)})", flush=True)

    # Coverage log
    total = len(merged)
    matched_gid = int((merged["game_id"].astype(str).str.len() > 0).sum())
    both_teams = int(((merged["team_id"].astype(str).str.len() > 0) &
                      (merged["opponent_team_id"].astype(str).str.len() > 0)).sum())
    print(f"[COVERAGE] rows_in={total}, matched_game_id={matched_gid}, unmatched={total - matched_gid}, with_team_and_opponent={both_teams}", flush=True)

    print(">> END: project_prep.py", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Ensure a clear failure message in the 06 summary log
        print(repr(e), file=sys.stderr, flush=True)
        raise
