#!/usr/bin/env python3
# scripts/inject_game_ids_from_schedule.py
#
# Overwrite (or create) game_id in each of the four projection files by
# matching their team_id to home_team_id OR away_team_id in data/raw/todaysgames_normalized.csv.
#
# Files:
#   - data/_projections/batter_props_projected_final.csv
#   - data/_projections/batter_props_expanded_final.csv
#   - data/_projections/pitcher_props_projected_final.csv
#   - data/_projections/pitcher_mega_z_final.csv
#
# Rules:
#   - No fabrication: if a team_id maps to 0 or multiple game_ids (e.g., doubleheader),
#     set game_id to NA and report counts.
#   - Always overwrite existing game_id column with the newly computed value.

from pathlib import Path
import sys
import pandas as pd

SCHEDULE_FILE = Path("data/raw/todaysgames_normalized.csv")

TARGET_FILES = [
    Path("data/_projections/batter_props_projected_final.csv"),
    Path("data/_projections/batter_props_expanded_final.csv"),
    Path("data/_projections/pitcher_props_projected_final.csv"),
    Path("data/_projections/pitcher_mega_z_final.csv"),
]

REQUIRED_SCHED_COLS = {"game_id", "home_team_id", "away_team_id"}
REQUIRED_TARGET_COLS = {"team_id"}

def die(msg: str) -> None:
    sys.stderr.write(f"ERROR: {msg}\n")
    sys.exit(1)

def load_schedule(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"Missing schedule file: {path}")
    df = pd.read_csv(path)
    missing = REQUIRED_SCHED_COLS - set(df.columns)
    if missing:
        die(f"{path} missing required columns: {sorted(missing)}")
    for c in ["game_id", "home_team_id", "away_team_id"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def build_team_to_game_map(schedule: pd.DataFrame) -> tuple[dict, set]:
    home = schedule[["home_team_id", "game_id"]].rename(columns={"home_team_id": "team_id"})
    away = schedule[["away_team_id", "game_id"]].rename(columns={"away_team_id": "team_id"})
    long = pd.concat([home, away], ignore_index=True).dropna(subset=["team_id"]).copy()

    grouped = long.groupby("team_id", dropna=True)["game_id"].apply(lambda s: set(s.dropna().tolist()))

    unique_map: dict = {}
    ambiguous_ids: set = set()
    for tid, games in grouped.items():
        tid_int = int(tid)
        if len(games) == 1:
            unique_map[tid_int] = int(next(iter(games)))
        elif len(games) > 1:
            ambiguous_ids.add(tid_int)
    return unique_map, ambiguous_ids

def overwrite_game_id_for_file(path: Path, team_to_game: dict, ambiguous_ids: set) -> None:
    if not path.exists():
        die(f"Missing target file: {path}")
    df = pd.read_csv(path)

    missing = REQUIRED_TARGET_COLS - set(df.columns)
    if missing:
        die(f"{path} missing required columns: {sorted(missing)}")

    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")

    # Compute new game_id (overwrite policy)
    new_game_id = df["team_id"].map(
        lambda x: team_to_game.get(int(x)) if pd.notna(x) and int(x) in team_to_game else pd.NA
    ).astype("Int64")

    # Null out ambiguous team_ids
    mask_ambig = df["team_id"].isin(list(ambiguous_ids))
    new_game_id.loc[mask_ambig] = pd.NA

    # Overwrite or create game_id column
    if "game_id" in df.columns:
        df["game_id"] = new_game_id
    else:
        df.insert(len(df.columns), "game_id", new_game_id)

    total = len(df)
    assigned = df["game_id"].notna().sum()
    unmatched_ids = set(df.loc[df["game_id"].isna() & df["team_id"].notna(), "team_id"].dropna().astype(int)) - ambiguous_ids
    ambiguous_hits = set(df.loc[mask_ambig, "team_id"].dropna().astype(int))

    print(
        f"✅ {path}: rows={total}, game_id_assigned={assigned}, "
        f"unmatched_team_ids={len(unmatched_ids)}, ambiguous_team_ids={len(ambiguous_hits)}"
    )

    df.to_csv(path, index=False)

def main() -> None:
    schedule = load_schedule(SCHEDULE_FILE)
    team_to_game, ambiguous_ids = build_team_to_game_map(schedule)
    for f in TARGET_FILES:
        overwrite_game_id_for_file(f, team_to_game, ambiguous_ids)

if __name__ == "__main__":
    main()
