#!/usr/bin/env python3
"""
Normalize ID columns to strings for all 06-projection inputs.

What it does (in-place):
- Ensures every column that looks like an ID is stored as a clean string
  (e.g., "147", not 147 or 147.0; blanks instead of NaN).
- Writes files back to their original locations (atomic temp -> replace).
- Prints a concise report of which columns were touched.

Intended to run as the FIRST step of the 06 workflow, before project_prep.py.
"""

from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path
from typing import List, Dict

DATA_DIR = Path("data")

# Known inputs that feed 06 (add others here if needed)
TARGETS: List[Path] = [
    # todaysgames used by project_prep
    DATA_DIR / "_projections" / "todaysgames_normalized_fixed.csv",

    # pitchers master used by project_prep
    DATA_DIR / "Data" / "pitchers.csv",

    # static stadium master (source of truth for team_id)
    DATA_DIR / "manual" / "stadium_master.csv",

    # (Optional) schedule that some steps read; harmless to normalize
    DATA_DIR / "schedule" / "mlb_schedule.csv",

    # If you have a team mapping file that feeds any merges, include it:
    DATA_DIR / "manual" / "team_master.csv",  # ignore if it doesn’t exist
]

# Columns we will coerce to string if present (exact matches)
ID_COLUMNS = {
    # Team IDs
    "team_id", "home_team_id", "away_team_id",

    # Player/ Pitcher IDs
    "player_id", "pitcher_home_id", "pitcher_away_id",

    # Game IDs (kept as strings to avoid 776436.0 junk)
    "game_id",

    # Misc variants commonly seen across your repo; harmless if absent
    "opponent_team_id", "batting_team_id", "fielding_team_id",
    "home_pitcher_id", "away_pitcher_id",
    "home_batter_id", "away_batter_id",
}

# Also coerce any column whose name contains one of these substrings
# (covers future files without having to edit this script)
SUBSTRINGS = [
    "team_id",
    "player_id",
    "pitcher_id",
    "_team_",
    "_pitcher_",
    "_batter_",
    "game_id",
]

def looks_like_id(colname: str) -> bool:
    cl = colname.strip().lower()
    if cl in ID_COLUMNS:
        return True
    return any(s in cl for s in SUBSTRINGS)

def normalize_scalar(v) -> str:
    """Convert any scalar to a clean ID string:
       - NaN/None -> ""
       - numeric like 147.0 -> "147"
       - strings are stripped; "147.0" -> "147" if it’s a float-format string
    """
    if pd.isna(v):
        return ""
    # If it's already a string, strip and trim a trailing ".0"
    if isinstance(v, str):
        s = v.strip()
        # common floaty strings like "147.0" -> "147"
        if s.endswith(".0"):
            try:
                f = float(s)
                if f.is_integer():
                    return str(int(f))
            except Exception:
                pass
        return s
    # Numbers: prefer integer formatting when possible
    if isinstance(v, (int,)):
        return str(v)
    if isinstance(v, (float,)):
        if pd.isna(v):
            return ""
        if v.is_integer():
            return str(int(v))
        # Non-integer floats—rare for IDs—format without scientific notation
        return ("%.0f" % v) if abs(v - round(v)) < 1e-9 else str(v)
    # Fallback to string
    return str(v).strip()

def normalize_id_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Coerce all ID-like columns in df to string. Returns a map of col->dtype_before.
    """
    touched: Dict[str, str] = {}
    for col in df.columns:
        if looks_like_id(col):
            before = str(df[col].dtype)
            # Skip if already object/string but still clean values
            df[col] = df[col].map(normalize_scalar)
            after = str(df[col].dtype)
            touched[col] = f"{before} -> {after}"
    return touched

def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)

def process_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"⚠️  SKIP (unable to read): {path} -> {e}")
        return

    # Clean headers
    df.columns = [c.strip() for c in df.columns]

    touched = normalize_id_columns(df)
    if touched:
        atomic_write_csv(df, path)
        cols = ", ".join(sorted(touched.keys()))
        print(f"✅ Normalized {path} | cols: {cols}")
    else:
        print(f"ℹ️  No ID-like columns to normalize in {path}")

def main(argv: List[str]) -> int:
    # Allow optional extra file paths via CLI
    extra = [Path(p) for p in argv[1:]] if len(argv) > 1 else []
    targets = [p for p in TARGETS + extra if p is not None]
    if not targets:
        print("No targets specified.")
        return 0

    print("=== normalize_ids_for_projection: START ===")
    for p in targets:
        process_file(p)
    print("=== normalize_ids_for_projection: DONE ===")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
