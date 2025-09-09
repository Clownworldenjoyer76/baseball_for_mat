#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import sys

DATA = Path("data")
PROJ = DATA / "_projections"
SUMM = Path("summaries") / "projections"
SUMM.mkdir(parents=True, exist_ok=True)

TODAY_GAMES = PROJ / "todaysgames_normalized_fixed.csv"
MEGA_Z      = PROJ / "pitcher_mega_z.csv"          # pre-clean
MEGA_Z_FIX  = PROJ / "pitcher_mega_z_fixed.csv"    # post-fix fallback
COVERAGE_CSV = SUMM / "mega_z_starter_coverage.csv"
MISSING_CSV  = SUMM / "mega_z_starter_missing.csv"

ID_FIELDS = [
    "pitcher_home_id","pitcher_away_id","player_id","team_id","home_team_id","away_team_id","game_id"
]

def to_str(df: pd.DataFrame) -> pd.DataFrame:
    for c in ID_FIELDS:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def read_games() -> pd.DataFrame:
    df = pd.read_csv(TODAY_GAMES, dtype=str, low_memory=False)
    df = to_str(df)
    # Only the starter IDs & teams are necessary
    keep = [c for c in ["game_id","home_team_id","away_team_id","pitcher_home_id","pitcher_away_id"] if c in df.columns]
    return df[keep].copy()

def read_mega() -> pd.DataFrame:
    path = MEGA_Z if MEGA_Z.exists() else MEGA_Z_FIX
    if not path.exists():
        raise FileNotFoundError(f"Missing mega_z file at {MEGA_Z} or {MEGA_Z_FIX}")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df = to_str(df)
    # Expect a player_id column
    if "player_id" not in df.columns:
        raise KeyError("pitcher_mega_z missing 'player_id' column")
    return df[["player_id"]].dropna().drop_duplicates().copy()

def main():
    try:
        games = read_games()
        mega  = read_mega()

        starters = pd.Series(pd.unique(pd.concat([games["pitcher_home_id"], games["pitcher_away_id"]], ignore_index=True))).dropna()
        starters = starters.astype(str)

        have = set(mega["player_id"].astype(str).tolist())
        want = set(starters.tolist())
        missing = sorted(want - have)
        covered = sorted(want & have)

        # Always write coverage + missing artifacts
        pd.DataFrame({"starter_player_id": sorted(list(want))}).to_csv(COVERAGE_CSV, index=False)
        pd.DataFrame({"missing_player_id": missing}).to_csv(MISSING_CSV, index=False)

        if missing:
            msg = f"Starter coverage failure: {len(missing)} starter(s) absent in pitcher_mega_z."
            print(msg)
            print(f"Wrote {COVERAGE_CSV} and {MISSING_CSV} with details.")
            # Hard fail to stop the pipeline, but after writing files
            raise RuntimeError(msg)
        else:
            print("Starter coverage OK: all starters present in pitcher_mega_z.")
            print(f"Wrote {COVERAGE_CSV} (no missing).")

    except Exception as e:
        # Best-effort: if anything blew up before writing, try to salvage what we can
        if not MISSING_CSV.exists():
            try:
                # Attempt to emit at least empty placeholders to avoid guessing later
                pd.DataFrame({"missing_player_id": []}).to_csv(MISSING_CSV, index=False)
            except Exception:
                pass
        if not COVERAGE_CSV.exists():
            try:
                pd.DataFrame({"starter_player_id": []}).to_csv(COVERAGE_CSV, index=False)
            except Exception:
                pass
        # Re-raise to keep the failure visible
        raise

if __name__ == "__main__":
    sys.exit(main())
