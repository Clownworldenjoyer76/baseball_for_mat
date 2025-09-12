#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Purpose
#   Inject team_id and game_id into:
#     - data/_projections/batter_props_projected_final.csv
#     - data/_projections/batter_props_expanded_final.csv
#   using:
#     - data/raw/lineups.csv            (player_id, team_id)
#     - data/raw/todaysgames_normalized.csv (game_id, home_team_id, away_team_id)
#
# Behavior
#   - Build team_id -> game_id map from schedule (home/away).
#   - Join lineups (player_id -> team_id) to get each batter's team_id.
#   - Map team_id -> game_id; attach to batters.
#   - Preserve any existing non-null team_id/game_id in batter files; only fill missing/UNKNOWN.
#   - Write diagnostics to summaries/07_final/.
#   - Overwrite the two _final batter files in place.
#
# Outputs (overwritten in place)
#   - data/_projections/batter_props_projected_final.csv
#   - data/_projections/batter_props_expanded_final.csv
#
# Diagnostics
#   - summaries/07_final/prep_injection_log.txt (human-readable log)
#   - summaries/07_final/missing_batters_in_lineups.csv
#   - summaries/07_final/teams_missing_from_schedule.csv
#   - summaries/07_final/batters_missing_game_ids_after_injection.csv

from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
import sys
from datetime import datetime

RAW_DIR   = Path("data/raw")
PROJ_DIR  = Path("data/_projections")
SUM_DIR   = Path("summaries/07_final")

SCHEDULE  = RAW_DIR / "todaysgames_normalized.csv"     # cols: game_id, home_team_id, away_team_id
LINEUPS   = RAW_DIR / "lineups.csv"                     # cols: player_id, team_id

BATTERS_PROJECTED = PROJ_DIR / "batter_props_projected_final.csv"
BATTERS_EXPANDED  = PROJ_DIR / "batter_props_expanded_final.csv"

LOG_FILE = SUM_DIR / "prep_injection_log.txt"

REQUIRED_SCHEDULE_COLS = ["game_id", "home_team_id", "away_team_id"]
REQUIRED_LINEUPS_COLS  = ["player_id", "team_id"]
REQUIRED_BATTER_KEYS   = ["player_id"]

def log(msg: str):
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")
    print(msg)

def read_csv(path: Path, want: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(str(path))
    df = pd.read_csv(path, low_memory=False)
    if want:
        missing = [c for c in want if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing required columns: {missing}")
    return df

def to_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df

def coalesce_fill(target: pd.Series, filler: pd.Series) -> pd.Series:
    # Fill null/empty/"UNKNOWN" in target from filler
    t = target.astype("string")
    f = filler.astype("string")
    need = t.isna() | (t.str.len() == 0) | (t.str.upper() == "UNKNOWN")
    out = t.copy()
    out.loc[need] = f.loc[need]
    return out

def build_team_to_game_map(schedule: pd.DataFrame) -> pd.DataFrame:
    # Melt home/away into team_id -> game_id
    sch = schedule.copy()
    sch = to_str(sch, ["game_id", "home_team_id", "away_team_id"])
    home = sch.rename(columns={"home_team_id": "team_id"})[["game_id", "team_id"]]
    away = sch.rename(columns={"away_team_id": "team_id"})[["game_id", "team_id"]]
    team_game = (
        pd.concat([home, away], ignore_index=True)
        .dropna(subset=["team_id", "game_id"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return to_str(team_game, ["team_id", "game_id"])

def inject_into_batter_file(bat_path: Path,
                            lineup_team_game: pd.DataFrame,
                            write_missing=True) -> dict:
    """
    Inject team_id/game_id into a single batter file, preserving existing non-null values.
    Returns a small dict of counts for logging/diagnostics.
    """
    df = read_csv(bat_path, want=REQUIRED_BATTER_KEYS)
    orig_rows = len(df)
    df = to_str(df, ["player_id"])
    # Ensure columns exist to fill into
    if "team_id" not in df.columns:
        df["team_id"] = pd.NA
    if "game_id" not in df.columns:
        df["game_id"] = pd.NA
    df = to_str(df, ["team_id", "game_id"])

    # Merge in lookup by player_id -> (team_id, game_id) from lineups + schedule
    lk = lineup_team_game.copy()
    lk = to_str(lk, ["player_id", "team_id", "game_id"])
    merged = df.merge(lk[["player_id", "team_id", "game_id"]],
                      on="player_id", how="left",
                      suffixes=("", "_from_lookup"))

    # Preserve existing non-null values; only fill holes/UNKNOWN from lookup
    merged["team_id"] = coalesce_fill(merged["team_id"], merged["team_id_from_lookup"])
    merged["game_id"] = coalesce_fill(merged["game_id"], merged["game_id_from_lookup"])

    # Drop helper columns
    drop_cols = [c for c in merged.columns if c.endswith("_from_lookup")]
    merged.drop(columns=drop_cols, inplace=True)

    # Diagnostics
    missing_team = merged["team_id"].isna() | (merged["team_id"].str.len() == 0)
    missing_game = merged["game_id"].isna() | (merged["game_id"].str.len() == 0)
    missing_both = missing_team | missing_game

    # Write back
    merged.to_csv(bat_path, index=False)

    stats = {
        "file": str(bat_path),
        "rows": orig_rows,
        "filled_team_id": int((~df["team_id"].astype("string").fillna("").isin(["", "UNKNOWN"])) &
                              (df["team_id"].astype("string").fillna("") != merged["team_id"].astype("string").fillna("")).sum()),
        "filled_game_id": int((~df["game_id"].astype("string").fillna("").isin(["", "UNKNOWN"])) &
                              (df["game_id"].astype("string").fillna("") != merged["game_id"].astype("string").fillna("")).sum()),
        "missing_team_rows": int(missing_team.sum()),
        "missing_game_rows": int(missing_game.sum()),
        "missing_any_rows": int(missing_both.sum()),
    }

    if write_missing and missing_both.any():
        merged.loc[missing_both, ["player_id", "team_id", "game_id"]].drop_duplicates().to_csv(
            SUM_DIR / f"batters_missing_game_ids_after_injection_{bat_path.name}.csv", index=False
        )

    return stats

def main() -> int:
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text("", encoding="utf-8")  # reset log

    log(f">> START prepare_daily_projection_inputs.py ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})")

    # Load inputs
    schedule = read_csv(SCHEDULE, want=REQUIRED_SCHEDULE_COLS)
    lineups  = read_csv(LINEUPS,  want=REQUIRED_LINEUPS_COLS)

    schedule = to_str(schedule, REQUIRED_SCHEDULE_COLS)
    lineups  = to_str(lineups, REQUIRED_LINEUPS_COLS)

    # Build team -> game map
    team_game = build_team_to_game_map(schedule)
    log(f"[INFO] Schedule teams today: {team_game['team_id'].nunique()} | games={schedule['game_id'].nunique()}")

    # Enrich lineups with game_id
    lineup_enriched = lineups.merge(team_game, on="team_id", how="left")
    # Diagnostics: players not in lineups we can't help; teams not in schedule
    teams_missing = lineup_enriched.loc[lineup_enriched["game_id"].isna(), ["team_id"]].drop_duplicates()
    if not teams_missing.empty:
        teams_missing.to_csv(SUM_DIR / "teams_missing_from_schedule.csv", index=False)
        log(f"[WARN] {len(teams_missing)} team(s) from lineups missing in schedule -> summaries/07_final/teams_missing_from_schedule.csv")

    # Batter files may have players not present in lineups; log that after inspecting each file
    # But first aggregate set of players we have team mapping for:
    have_map_pids = set(lineup_enriched["player_id"].dropna().astype(str))

    # Inject into both batter files
    stats_proj = inject_into_batter_file(BATTERS_PROJECTED, lineup_enriched, write_missing=True)
    stats_exp  = inject_into_batter_file(BATTERS_EXPANDED,  lineup_enriched, write_missing=True)

    # Post diagnostics: who in files lacks a lineup mapping?
    for bat_path in [BATTERS_PROJECTED, BATTERS_EXPANDED]:
        df = read_csv(bat_path, want=["player_id"])
        df = to_str(df, ["player_id", "team_id", "game_id"])
        missing_in_lineups = df.loc[~df["player_id"].isin(have_map_pids), ["player_id"]].drop_duplicates()
        if not missing_in_lineups.empty:
            missing_in_lineups.to_csv(
                SUM_DIR / f"missing_batters_in_lineups_{bat_path.name}.csv", index=False
            )
            log(f"[WARN] {len(missing_in_lineups)} batter(s) in {bat_path.name} not found in lineups -> summaries/07_final/missing_batters_in_lineups_{bat_path.name}.csv")

    # Summary
    log("[SUMMARY] Injection results:")
    for s in (stats_proj, stats_exp):
        log(f"  - {s['file']}: rows={s['rows']}, "
            f"missing_team_rows={s['missing_team_rows']}, missing_game_rows={s['missing_game_rows']}, "
            f"missing_any_rows={s['missing_any_rows']}")

    log("[OK] prepare_daily_projection_inputs.py completed")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        SUM_DIR.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(f"[FAIL] {repr(e)}\n")
        print(e)
        sys.exit(1)
