#!/usr/bin/env python3
# Aggregates daily batter/pitcher projections into game-level expected runs.
# Input (produced upstream):
#   - data/end_chain/final/batter_event_probabilities.csv   (required)
#   - data/end_chain/final/pitcher_event_probabilities.csv  (optional; only for logging)
# Output:
#   - data/end_chain/final/game_score_projections.csv
#
# Robust changes:
#   - Normalize keys (game_id, team_id) to integer dtype (no 776271.0 string bleed).
#   - Compute expected runs from columns if needed.
#   - Drop games that do not have exactly 2 distinct teams and LOG them, rather than error.

from __future__ import annotations

import pandas as pd
from pathlib import Path

SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")
SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATTER_EVENTS = OUT_DIR / "batter_event_probabilities.csv"
PITCHER_EVENTS = OUT_DIR / "pitcher_event_probabilities.csv"  # optional for aggregate
OUT_FILE = OUT_DIR / "game_score_projections.csv"

def write_text(p: Path, txt: str) -> None:
    p.write_text(txt, encoding="utf-8")

def require(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing columns: {missing}")

def to_num(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def ensure_expected_runs_batter(bat: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure column 'expected_runs_batter' exists.
    Preference order:
      1) Use existing 'expected_runs_batter'
      2) Compute runs_per_pa * proj_pa_used if both present
      3) Compute runs_per_pa from event probabilities * linear weights, then multiply by proj_pa_used
    """
    if "expected_runs_batter" in bat.columns and bat["expected_runs_batter"].notna().any():
        to_num(bat, ["expected_runs_batter"])
        bat["expected_runs_batter"] = bat["expected_runs_batter"].fillna(0.0).clip(lower=0)
        return bat

    # Option 2: runs_per_pa * proj_pa_used
    if {"runs_per_pa", "proj_pa_used"}.issubset(bat.columns):
        to_num(bat, ["runs_per_pa", "proj_pa_used"])
        bat["expected_runs_batter"] = (
            bat["runs_per_pa"].fillna(0.0).clip(lower=0) *
            bat["proj_pa_used"].fillna(0.0).clip(lower=0)
        )
        return bat

    # Option 3: derive runs_per_pa from p_* columns
    lw = {"BB":0.33, "1B":0.47, "2B":0.77, "3B":1.04, "HR":1.40, "OUT":0.0}
    need_p = ["p_bb", "p_1b", "p_2b", "p_3b", "p_hr", "p_out", "proj_pa_used"]
    missing_p = [c for c in need_p if c not in bat.columns]
    if missing_p:
        raise RuntimeError(
            "Cannot construct expected_runs_batter: need either "
            "'expected_runs_batter' or ('runs_per_pa' & 'proj_pa_used') or "
            f"event probs {need_p}; missing={missing_p}"
        )

    to_num(bat, need_p)
    bat["runs_per_pa"] = (
        bat["p_bb"].fillna(0)*lw["BB"] +
        bat["p_1b"].fillna(0)*lw["1B"] +
        bat["p_2b"].fillna(0)*lw["2B"] +
        bat["p_3b"].fillna(0)*lw["3B"] +
        bat["p_hr"].fillna(0)*lw["HR"] +
        bat["p_out"].fillna(0)*lw["OUT"]
    )
    bat["expected_runs_batter"] = (
        bat["runs_per_pa"].fillna(0.0).clip(lower=0) *
        bat["proj_pa_used"].fillna(0.0).clip(lower=0)
    )
    return bat

def normalize_keys(bat: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize game_id and team_id to nullable integer dtype (Int64) for clean grouping.
    """
    # Some upstream files may have strings like "776271.0" or plain strings; coerce safely.
    for key in ["game_id", "team_id"]:
        if key not in bat.columns:
            bat[key] = pd.NA
        bat[key] = pd.to_numeric(bat[key], errors="coerce").astype("Int64")
    # Team may be absent; keep string but fillna for grouping readability only.
    if "team" not in bat.columns:
        bat["team"] = ""
    else:
        bat["team"] = bat["team"].astype(str)
    return bat

def main():
    print("LOAD: batter & pitcher event files")
    if not BATTER_EVENTS.exists():
        raise RuntimeError(f"Missing {BATTER_EVENTS}; upstream batter projection step must run first.")

    bat = pd.read_csv(BATTER_EVENTS)
    pit_rows = 0
    if PITCHER_EVENTS.exists():
        pit = pd.read_csv(PITCHER_EVENTS)
        pit_rows = len(pit)

    require(bat, ["game_id", "team_id", "proj_pa_used"], str(BATTER_EVENTS))

    # Normalize keys and ensure expected_runs_batter
    bat = normalize_keys(bat)
    bat = ensure_expected_runs_batter(bat)

    print("AGG: sum expected runs by (game_id, team_id, team)")
    grouped = (
        bat.groupby(["game_id", "team_id", "team"], dropna=True)["expected_runs_batter"]
           .sum()
           .reset_index()
           .rename(columns={"expected_runs_batter": "expected_runs"})
    )

    # Validate exactly two teams per game_id; log & filter instead of hard error
    per_game_counts = grouped.groupby("game_id")["team_id"].nunique()
    bad_games = per_game_counts[per_game_counts != 2]

    if not bad_games.empty:
        # Save detailed offenders
        detail = grouped[grouped["game_id"].isin(bad_games.index)].copy()
        detail.to_csv(SUM_DIR / "gamescores_bad_games_detail.csv", index=False)
        write_text(SUM_DIR / "gamescores_bad_games.txt",
                   "bad_games=" + str({int(g): int(n) for g, n in bad_games.items()}))
        # Filter out incomplete/duplicated games
        grouped = grouped[~grouped["game_id"].isin(bad_games.index)].copy()

    # Sort and write
    grouped = grouped.sort_values(["game_id", "team_id"]).reset_index(drop=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(OUT_FILE, index=False)
    kept_games = grouped["game_id"].nunique()
    dropped_games = int(len(bad_games))

    print(f"WROTE: {len(grouped)} rows -> {OUT_FILE} (games kept={kept_games}, dropped={dropped_games}, pitcher_rows={pit_rows})")

    write_text(SUM_DIR / "status.txt",
               f"OK project_game_scores.py rows={len(grouped)} games_kept={kept_games} games_dropped={dropped_games} pit_rows={pit_rows}")
    write_text(SUM_DIR / "errors.txt", "")
    write_text(SUM_DIR / "summary.txt", f"rows={len(grouped)} out={OUT_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        SUM_DIR.mkdir(parents=True, exist_ok=True)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_text(SUM_DIR / "status.txt", "FAIL project_game_scores.py")
        write_text(SUM_DIR / "errors.txt", repr(e))
        write_text(SUM_DIR / "summary.txt", f"error={repr(e)}")
        raise
