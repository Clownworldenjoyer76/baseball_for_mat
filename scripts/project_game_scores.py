#!/usr/bin/env python3
# Purpose:
#   Build team-level expected runs for each game by aggregating the batter daily projections we just produced.
# Inputs:
#   data/_projections/batter_event_probs_daily.csv  (expected_runs_batter per row, with game_id/team_id/team)
# Outputs:
#   data/end_chain/final/game_score_projections.csv
# Diagnostics:
#   summaries/07_final/game_scores_status.txt, errors/log CSVs

from __future__ import annotations

from pathlib import Path
import pandas as pd

DAILY_DIR = Path("data/_projections")
SUM_DIR   = Path("summaries/07_final")
OUT_DIR   = Path("data/end_chain/final")

BATTER_DAILY = DAILY_DIR / "batter_event_probs_daily.csv"
OUT_FILE     = OUT_DIR / "game_score_projections.csv"

def write_text(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8")

def require(df: pd.DataFrame, cols: list[str], name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing columns: {miss}")

def main():
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("LOAD daily batter event projections")
    bat = pd.read_csv(BATTER_DAILY)
    require(bat, ["player_id","game_id","team_id","team","expected_runs_batter"], str(BATTER_DAILY))

    print("AGG -> team/game expected runs")
    team = (
        bat.groupby(["game_id","team_id","team"], dropna=True)["expected_runs_batter"]
           .sum()
           .reset_index()
           .rename(columns={"expected_runs_batter":"expected_runs"})
           .sort_values(["game_id","team_id"])
           .reset_index(drop=True)
    )

    team.to_csv(OUT_FILE, index=False)
    print(f"WROTE: {len(team)} rows -> {OUT_FILE}")
    write_text(SUM_DIR / "game_scores_status.txt", f"OK project_game_scores rows={len(team)}")
    write_text(SUM_DIR / "errors.txt", "")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        write_text(SUM_DIR / "game_scores_status.txt", "FAIL project_game_scores")
        write_text(SUM_DIR / "errors.txt", repr(e))
        raise
