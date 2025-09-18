#!/usr/bin/env python3
# Aggregates daily batter/pitcher projections into game-level expected runs.

import pandas as pd
from pathlib import Path

SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")
SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATTER_EVENTS = OUT_DIR / "batter_event_probabilities.csv"
PITCHER_EVENTS = OUT_DIR / "pitcher_event_probabilities.csv"  # optional
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
    if "expected_runs_batter" in bat.columns and bat["expected_runs_batter"].notna().any():
        to_num(bat, ["expected_runs_batter"])
        bat["expected_runs_batter"] = bat["expected_runs_batter"].fillna(0.0).clip(lower=0)
        return bat

    if {"runs_per_pa", "proj_pa_used"}.issubset(bat.columns):
        to_num(bat, ["runs_per_pa", "proj_pa_used"])
        bat["expected_runs_batter"] = (
            bat["runs_per_pa"].fillna(0.0).clip(lower=0) *
            bat["proj_pa_used"].fillna(0.0).clip(lower=0)
        )
        return bat

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
    if "team" not in bat.columns:
        bat["team"] = ""

    to_num(bat, ["game_id", "team_id", "proj_pa_used"])
    if bat["game_id"].isna().any() or bat["team_id"].isna().any():
        bad = bat.loc[bat["game_id"].isna() | bat["team_id"].isna(), ["player_id","team_id","game_id"]]
        bad.to_csv(SUM_DIR / "gamescores_null_keys.csv", index=False)
        raise RuntimeError(f"Null game_id/team_id in batter events; see {SUM_DIR/'gamescores_null_keys.csv'}")

    bat = ensure_expected_runs_batter(bat)

    print("AGG: sum expected runs by (game_id, team_id, team)")
    team = (
        bat.groupby(["game_id", "team_id", "team"], dropna=False)["expected_runs_batter"]
           .sum()
           .reset_index()
           .rename(columns={"expected_runs_batter": "expected_runs"})
           .sort_values(["game_id", "team_id"])
           .reset_index(drop=True)
    )

    # Defensive integrity checks
    teams_per_game = team.groupby("game_id")["team_id"].nunique()
    bad_games = teams_per_game[teams_per_game != 2]
    if not bad_games.empty:
        (SUM_DIR / "gamescores_bad_games.txt").write_text(
            f"bad_games={bad_games.to_dict()}\n", encoding="utf-8"
        )
        raise RuntimeError(f"Incomplete/duplicated teams per game. See {SUM_DIR/'gamescores_bad_games.txt'}")

    dup_pairs = team.duplicated(["game_id","team_id"])
    if dup_pairs.any():
        team.loc[dup_pairs].to_csv(SUM_DIR / "gamescores_dup_pairs.csv", index=False)
        raise RuntimeError(f"Duplicate (game_id, team_id) rows. See {SUM_DIR/'gamescores_dup_pairs.csv'}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    team.to_csv(OUT_FILE, index=False)
    print(f"WROTE: {len(team)} rows -> {OUT_FILE} (pitcher_rows={pit_rows})")

    write_text(SUM_DIR / "status.txt", f"OK project_game_scores.py rows={len(team)} pit_rows={pit_rows}")
    write_text(SUM_DIR / "errors.txt", "")
    write_text(SUM_DIR / "summary.txt", f"rows={len(team)} out={OUT_FILE}")

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
