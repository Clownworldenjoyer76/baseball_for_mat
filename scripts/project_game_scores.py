#!/usr/bin/env python3
# Aggregates daily batter/pitcher projections into game-level expected runs.
# Assumes upstream scripts produced:
#   - data/end_chain/final/batter_event_probabilities.csv
#   - data/end_chain/final/pitcher_event_probabilities.csv  (optional)
#
# Output:
#   - data/end_chain/final/game_score_projections.csv
#
# Behavior:
#   - Sums player expected runs to team level.
#   - Canonicalizes team name per (game_id, team_id), and aggregates by (game_id, team_id) ONLY.
#   - Identifies "bad games" (games with != 2 distinct team_ids) and drops them.
#   - Writes summaries/07_final/gamescores_bad_games.txt only when bad games exist.
#   - Removes that diagnostic when there are no bad games.

import pandas as pd
from pathlib import Path

SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")
SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATTER_EVENTS = OUT_DIR / "batter_event_probabilities.csv"
PITCHER_EVENTS = OUT_DIR / "pitcher_event_probabilities.csv"  # optional
OUT_FILE = OUT_DIR / "game_score_projections.csv"
BAD_GAMES_FILE = SUM_DIR / "gamescores_bad_games.txt"

def write_text(p: Path, txt: str) -> None:
    p.write_text(txt, encoding="utf-8")

def remove_file_if_exists(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
    except Exception:
        pass  # non-fatal

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
    Ensure 'expected_runs_batter' exists.
    Priority:
      (1) use 'expected_runs_batter' if present
      (2) runs_per_pa * proj_pa_used
      (3) derive runs_per_pa from event probabilities + linear weights
    """
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
    miss = [c for c in need_p if c not in bat.columns]
    if miss:
        raise RuntimeError(
            "Cannot construct expected_runs_batter: need either "
            "'expected_runs_batter' or ('runs_per_pa' & 'proj_pa_used') or "
            f"event probs {need_p}; missing={miss}"
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

def canonicalize_team_name(bat: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a single canonical team name per (game_id, team_id):
      - pick the first non-empty 'team' value if any, else empty string.
    """
    if "team" not in bat.columns:
        # no team names supplied; return empty mapping
        return bat[["game_id","team_id"]].drop_duplicates().assign(team="")
    # Choose first non-empty team string within each group
    def pick_name(s: pd.Series) -> str:
        for val in s:
            if isinstance(val, str) and val.strip():
                return val.strip()
        return ""
    names = (bat.groupby(["game_id","team_id"])["team"]
                .apply(pick_name)
                .reset_index())
    return names

def main():
    # Start with a clean diagnostic for this run
    remove_file_if_exists(BAD_GAMES_FILE)

    print("LOAD: batter & pitcher event files")
    if not BATTER_EVENTS.exists():
        raise RuntimeError(f"Missing {BATTER_EVENTS}; upstream batter projection step must run first.")

    bat = pd.read_csv(BATTER_EVENTS)
    pit_rows = 0
    if PITCHER_EVENTS.exists():
        pit = pd.read_csv(PITCHER_EVENTS)
        pit_rows = len(pit)

    require(bat, ["game_id", "team_id", "proj_pa_used"], str(BATTER_EVENTS))
    to_num(bat, ["game_id", "team_id", "proj_pa_used"])

    # Ensure expected runs present
    bat = ensure_expected_runs_batter(bat)

    # Build canonical team name per (game_id, team_id)
    name_map = canonicalize_team_name(bat)

    print("AGG: sum expected runs by (game_id, team_id)")
    team_runs = (
        bat.groupby(["game_id", "team_id"], dropna=True)["expected_runs_batter"]
           .sum()
           .reset_index()
           .rename(columns={"expected_runs_batter": "expected_runs"})
           .sort_values(["game_id", "team_id"])
           .reset_index(drop=True)
    )
    # attach canonical team label (may be empty string)
    team = team_runs.merge(name_map, on=["game_id","team_id"], how="left")

    # Identify bad games = games with != 2 distinct team_ids
    counts = team.groupby("game_id", dropna=False)["team_id"].nunique().reset_index(name="teams_present")
    bad = counts[counts["teams_present"] != 2].copy()

    if not bad.empty:
        # Per-game unique team_ids present (no duplicates)
        lines = ["# games with != 2 teams present; these were dropped from output",
                 f"# total_bad_games={bad.shape[0]}",
                 ""]
        merged = team.merge(bad[["game_id"]], on="game_id", how="inner")
        for gid, gdf in merged.groupby("game_id"):
            ids_unique = sorted({int(tid) for tid in gdf["team_id"].dropna().astype(int).tolist()})
            lines.append(f"{gid}: teams_present={ids_unique}")
        write_text(BAD_GAMES_FILE, "\n".join(lines))

        # keep only good games
        good_ids = counts[counts["teams_present"] == 2]["game_id"]
        team = team[team["game_id"].isin(good_ids)].copy()
        games_kept = good_ids.nunique()
        games_dropped = bad["game_id"].nunique()
    else:
        remove_file_if_exists(BAD_GAMES_FILE)
        games_kept = counts[counts["teams_present"] == 2]["game_id"].nunique()
        games_dropped = 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    team.to_csv(OUT_FILE, index=False)
    print(f"WROTE: {len(team)} rows -> {OUT_FILE} (games kept={games_kept}, dropped={games_dropped}, pitcher_rows={pit_rows})")

    # status/error summaries for CI convenience
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
