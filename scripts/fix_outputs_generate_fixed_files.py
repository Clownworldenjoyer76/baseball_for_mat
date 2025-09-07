#!/usr/bin/env python3
"""
Backfill missing pitcher IDs in data/raw/todaysgames_normalized.csv
using player master and (fallback) season pitchers, by normalized name.

Outputs:
  - data/_projections/todaysgames_normalized_fixed.csv  (on success)
  - summaries/projections/missing_pitcher_ids.csv       (if unresolved starters remain) -> exit 1
  - summaries/projections/missing_master_columns.txt    (if master/season lack any name field) -> exit 1
"""

from pathlib import Path
import sys
import pandas as pd

RAW_TODAY = Path("data/raw/todaysgames_normalized.csv")
MASTER    = Path("data/processed/player_team_master.csv")  # preferred map
SEASON_P  = Path("data/Data/pitchers.csv")                 # fallback map
SUM_DIR   = Path("summaries/projections")
OUT_DIR   = Path("data/_projections")

SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---- utilities ----

def normalize_name(val) -> str:
    if not isinstance(val, str):
        return ""
    s = val.strip().lower()
    # strip punctuation and accents-lite normalizations
    for ch in [".", ",", "'", "\"", "’", "`", "´"]:
        s = s.replace(ch, "")
    s = " ".join(s.split())  # collapse whitespace
    return s

def find_id_col(df: pd.DataFrame) -> str | None:
    # Common ID column names
    candidates = [
        "player_id", "mlb_id", "person_id", "id", "retro_id", "bbref_id"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None

def find_single_name_col(df: pd.DataFrame) -> str | None:
    # Common single-name columns
    candidates = [
        "player_name", "name", "full_name", "mlb_name", "display_name"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None

def build_name_norm_column(df: pd.DataFrame, *, new_col: str = "name_norm") -> bool:
    """
    Populate df[new_col] from either a single name column or first/last columns.
    Returns True if a name_norm was created, False otherwise.
    """
    if df.empty:
        return False
    single = find_single_name_col(df)
    if single:
        df[new_col] = df[single].map(normalize_name)
        return True
    # try first/last combinations
    first_opts = ["first_name", "firstname", "given_name"]
    last_opts  = ["last_name", "lastname", "family_name", "surname"]
    first = next((c for c in first_opts if c in df.columns), None)
    last  = next((c for c in last_opts  if c in df.columns), None)
    if first and last:
        df[new_col] = (df[first].fillna("") + " " + df[last].fillna("")).map(normalize_name)
        return True
    return False

def write_text(path: Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8")

# ---- main ----

def main() -> None:
    if not RAW_TODAY.exists():
        write_text(SUM_DIR / "missing_master_columns.txt",
                   f"Missing input file: {RAW_TODAY}")
        print(f"❌ Missing input: {RAW_TODAY}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(RAW_TODAY)

    # Ensure the schedule has the columns we expect to patch
    required_sched_cols = [
        "game_id",
        "home_team_id", "away_team_id",
        "pitcher_home", "pitcher_away",
        "pitcher_home_id", "pitcher_away_id",
    ]
    missing_sched = [c for c in required_sched_cols if c not in df.columns]
    if missing_sched:
        write_text(SUM_DIR / "missing_master_columns.txt",
                   f"todaysgames_normalized.csv missing columns: {missing_sched}")
        print(f"❌ Schedule missing columns: {missing_sched}", file=sys.stderr)
        sys.exit(1)

    # Load mapping sources (they might not exist; we handle that)
    master = pd.read_csv(MASTER) if MASTER.exists() else pd.DataFrame()
    season = pd.read_csv(SEASON_P) if SEASON_P.exists() else pd.DataFrame()

    # Prepare mapping tables with (name_norm, player_id)
    maps: list[pd.DataFrame] = []

    if not master.empty:
        # Build name_norm and identify ID column
        ok_name = build_name_norm_column(master, new_col="name_norm")
        id_col = find_id_col(master)
        if ok_name and id_col:
            maps.append(master[["name_norm", id_col]].rename(columns={id_col: "player_id"}))
        else:
            # If master present but unusable, log why (do not hard-fail; we still have season fallback)
            msg = []
            if not ok_name:
                msg.append("player_team_master.csv has no recognizable name column.")
            if not id_col:
                msg.append("player_team_master.csv has no recognizable player_id column.")
            if msg:
                write_text(SUM_DIR / "missing_master_columns.txt", " | ".join(msg))

    if not season.empty:
        ok_name = build_name_norm_column(season, new_col="name_norm")
        id_col = find_id_col(season)
        if ok_name and id_col:
            maps.append(season[["name_norm", id_col]].rename(columns={id_col: "player_id"}))
        else:
            msg = []
            if not ok_name:
                msg.append("pitchers.csv has no recognizable name column (nor first/last).")
            if not id_col:
                msg.append("pitchers.csv has no recognizable player_id column.")
            if msg:
                prev = (SUM_DIR / "missing_master_columns.txt").read_text(encoding="utf-8") if (SUM_DIR / "missing_master_columns.txt").exists() else ""
                write_text(SUM_DIR / "missing_master_columns.txt", (prev + ("\n" if prev else "") + " | ".join(msg)))

    # Combine maps (later sources won’t overwrite earlier matches when we lookup manually)
    map_df = pd.concat(maps, ignore_index=True).dropna(subset=["name_norm", "player_id"]).drop_duplicates()

    # Resolve missing pitcher IDs from names
    unresolved: list[dict] = []

    def resolve(side: str) -> None:
        pid_col = f"pitcher_{side}_id"
        name_col = f"pitcher_{side}"
        team_col = f"{side}_team_id"  # for diagnostics only

        # Normalize the name column (do NOT persist; use a temporary Series)
        name_norm_series = df[name_col].map(normalize_name)

        # Rows needing resolution
        need = df[pid_col].isna()
        if not need.any():
            return

        # Build a simple name->id map for vectorized map()
        name_to_id = dict(zip(map_df["name_norm"], map_df["player_id"]))
        # Attempt vectorized fill
        filled = name_norm_series.map(name_to_id)

        # Apply only where missing
        df.loc[need, pid_col] = filled[need]

        # Track any still-missing rows
        still_need = df[pid_col].isna()
        for idx in df[still_need].index:
            unresolved.append({
                "game_id":      df.at[idx, "game_id"],
                "team_id":      df.at[idx, team_col],
                "pitcher_side": side,
                "pitcher_name": df.at[idx, name_col]
            })

    resolve("home")
    resolve("away")

    if unresolved:
        miss_path = SUM_DIR / "missing_pitcher_ids.csv"
        pd.DataFrame(unresolved).to_csv(miss_path, index=False)
        print(f"❌ Unresolved pitcher IDs ({len(unresolved)}). See {miss_path}", file=sys.stderr)
        sys.exit(1)

    # Success: write fixed copy for downstream steps
    out_path = OUT_DIR / "todaysgames_normalized_fixed.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Fixed todaysgames_normalized written: {out_path}")

if __name__ == "__main__":
    main()
