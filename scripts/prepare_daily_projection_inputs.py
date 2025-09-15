#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Goal:
#   Ensure batter *_final daily files have string 'team_id' and 'game_id' columns.
#   - Pull team_id by player_id from data/raw/lineups.csv (if missing)
#   - Pull game_id by team_id from data/raw/todaysgames_normalized.csv
#   - Never crash on absent/suffixed columns; coalesce safely.
#   - Emit warnings + CSVs listing rows still missing team_id / game_id.
#
# Inputs:
#   data/_projections/batter_props_projected_final.csv
#   data/_projections/batter_props_expanded_final.csv
#   data/raw/lineups.csv  (must include: player_id, team_id)
#   data/raw/todaysgames_normalized.csv (includes: game_id, home_team_id, away_team_id)
#
# Outputs (overwrite in place):
#   data/_projections/batter_props_projected_final.csv
#   data/_projections/batter_props_expanded_final.csv
#
# Diagnostics:
#   summaries/07_final/missing_team_id_in_<file>.csv
#   summaries/07_final/missing_game_id_in_<file>.csv
#   summaries/07_final/prep_injection_log.txt

from __future__ import annotations

import sys
import pandas as pd
from pathlib import Path

# Paths
PROJ_DIR = Path("data/_projections")
RAW_DIR = Path("data/raw")
SUM_DIR = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_PROJECTED = PROJ_DIR / "batter_props_projected_final.csv"
BATTERS_EXPANDED  = PROJ_DIR / "batter_props_expanded_final.csv"
LINEUPS_CSV       = RAW_DIR / "lineups.csv"
TGN_CSV           = RAW_DIR / "todaysgames_normalized.csv"

LOG_FILE = SUM_DIR / "prep_injection_log.txt"

def log(msg: str) -> None:
    print(msg, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")

def read_csv_force_str(path: Path) -> pd.DataFrame:
    """Read a CSV with all columns as string (object) dtype."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    # Normalize whitespace and None-like placeholders to empty string
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
        df[c] = df[c].replace({"None": "", "nan": "", "NaN": ""})
    return df

def coalesce_series(a: pd.Series | None, b: pd.Series | None) -> pd.Series:
    """Return first non-empty (not NA/empty-string) between a and b (both may be None)."""
    if a is None and b is None:
        # produce empty series
        return pd.Series([], dtype="object")
    if a is None:
        a = pd.Series([""] * len(b), index=b.index, dtype="object")
    if b is None:
        b = pd.Series([""] * len(a), index=a.index, dtype="object")
    a = a.astype(str)
    b = b.astype(str)
    out = a.where(a.str.len() > 0, b)
    out = out.fillna("").astype(str)
    return out

def build_team_to_game_map(tgn: pd.DataFrame) -> pd.DataFrame:
    """Explode todaysgames_normalized into (team_id, game_id) rows."""
    need = {"game_id", "home_team_id", "away_team_id"}
    missing = sorted(list(need - set(tgn.columns)))
    if missing:
        raise RuntimeError(f"{TGN_CSV} missing columns: {missing}")

    # Keep only the columns we need and force strings
    tgn = tgn[["game_id", "home_team_id", "away_team_id"]].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()

    home = tgn.rename(columns={"home_team_id": "team_id"})[["game_id", "team_id"]].copy()
    away = tgn.rename(columns={"away_team_id": "team_id"})[["game_id", "team_id"]].copy()
    team_game = pd.concat([home, away], ignore_index=True)
    # Drop empties
    team_game["team_id"] = team_game["team_id"].replace({"None": "", "nan": "", "NaN": ""})
    team_game = team_game[team_game["team_id"].astype(str).str.len() > 0].drop_duplicates()
    return team_game

def inject_team_and_game(df: pd.DataFrame, name_for_logs: str,
                         lineups: pd.DataFrame, team_game_map: pd.DataFrame) -> pd.DataFrame:
    """
    - Ensure 'player_id' exists.
    - Coalesce/attach 'team_id' using any existing df team_id and the lineups merge.
    - Attach 'game_id' via team_id using the team_game_map (home/away exploded).
    - Emit missing lists.
    """
    if "player_id" not in df.columns:
        raise RuntimeError(f"{name_for_logs} missing required column: player_id")

    # Force all string dtypes early
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Merge to bring in team_id from lineups (rename to avoid suffix guessing)
    li = lineups.copy()
    li = li.rename(columns={"team_id": "team_id_lineups"})
    li = li[["player_id", "team_id_lineups"]].copy()

    merged = df.merge(li, on="player_id", how="left")

    # Coalesce team_id: prefer existing non-empty df['team_id'], else lineups
    existing_team = merged["team_id"] if "team_id" in merged.columns else None
    from_lineups  = merged["team_id_lineups"] if "team_id_lineups" in merged.columns else None
    merged["team_id"] = coalesce_series(existing_team, from_lineups)

    # Normalize team_id
    merged["team_id"] = merged["team_id"].fillna("").astype(str).str.strip()

    # Attach game_id from exploded map if missing or empty
    # Keep any pre-existing 'game_id' but fill missing via map
    merged = merged.merge(team_game_map, on="team_id", how="left", suffixes=("", "_from_map"))
    # Coalesce: prefer any existing non-empty game_id in df, else from map
    existing_gid = merged["game_id"] if "game_id" in merged.columns else None
    from_map     = merged["game_id_from_map"] if "game_id_from_map" in merged.columns else None
    merged["game_id"] = coalesce_series(existing_gid, from_map)
    if "game_id_from_map" in merged.columns:
        merged.drop(columns=["game_id_from_map"], inplace=True)

    # Warn and write diagnostics for any still-missing team_id / game_id
    miss_team = merged.loc[merged["team_id"].astype(str).str.len() == 0, ["player_id"]].drop_duplicates()
    miss_gid  = merged.loc[(merged["game_id"].astype(str).str.len() == 0),
                           ["player_id", "team_id"]].drop_duplicates()

    if len(miss_team) > 0:
        out = SUM_DIR / f"missing_team_id_in_{name_for_logs}.csv"
        miss_team.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_team)} rows missing team_id ({out})")

    if len(miss_gid) > 0:
        out = SUM_DIR / f"missing_game_id_in_{name_for_logs}.csv"
        miss_gid.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_gid)} rows missing game_id ({out})")

    log(f"[INFO] {name_for_logs}: missing team_id={len(miss_team)}, missing game_id={len(miss_gid)}")

    return merged

def main() -> None:
    # Fresh log
    LOG_FILE.write_text("", encoding="utf-8")
    log("PREP: injecting team_id and game_id into batter *_final.csv")

    # Load inputs
    bat_proj = read_csv_force_str(BATTERS_PROJECTED)
    bat_exp  = read_csv_force_str(BATTERS_EXPANDED)
    lineups  = read_csv_force_str(LINEUPS_CSV)
    tgn      = read_csv_force_str(TGN_CSV)

    # Build team_id -> game_id mapping (home/away exploded)
    team_game_map = build_team_to_game_map(tgn)

    # Process both batter files
    bat_proj_out = inject_team_and_game(bat_proj, "batter_props_projected_final.csv", lineups, team_game_map)
    bat_exp_out  = inject_team_and_game(bat_exp,  "batter_props_expanded_final.csv",  lineups, team_game_map)

    # Persist back to the same paths (keep column order stable: put team_id, game_id at the end if they were new)
    def write_back(df_before: pd.DataFrame, df_after: pd.DataFrame, path: Path) -> None:
        # Preserve original column order + ensure team_id/game_id present at end if not originally present
        cols = list(df_before.columns)
        for add_col in ["team_id", "game_id"]:
            if add_col not in cols:
                cols.append(add_col)
        # Some rows may have extra merge helper columns removed already; guard with intersection
        cols_final = [c for c in cols if c in df_after.columns]
        df_after[cols_final].to_csv(path, index=False)

    write_back(bat_proj, bat_proj_out, BATTERS_PROJECTED)
    write_back(bat_exp,  bat_exp_out,  BATTERS_EXPANDED)

    log(f"OK: wrote {BATTERS_PROJECTED} and {BATTERS_EXPANDED}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Keep the log tidy but explicit
        msg = f"[ERROR] prepare_daily_projection_inputs failed: {repr(e)}"
        print(msg)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
        # Propagate non-zero exit for CI
        raise
