#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
import numpy as np

DATA_DIR = Path("data")
CLEAN_DIR = DATA_DIR / "cleaned"
RAW_DIR = DATA_DIR / "raw"
PROJ_DIR = DATA_DIR / "_projections"
SUM_DIR = Path("summaries") / "projections"

SOURCE_FILE = CLEAN_DIR / "pitchers_normalized_cleaned.csv"   # existing source
TODAYS_FILE = RAW_DIR / "startingpitchers_with_opp_context.csv"
OUT_FILE    = PROJ_DIR / "pitcher_mega_z.csv"
MISS_FILE   = SUM_DIR / "mega_z_starter_missing.csv"          # for starter_coverage_guard.py
COVER_FILE  = SUM_DIR / "mega_z_starter_coverage.csv"         # optional coverage map

REQUIRED_KEYS = ["player_id"]  # keep light; dtype will be enforced to str

def _to_str(series: pd.Series) -> pd.Series:
    # robust string normalization (keeps NaN as NaN, strips spaces)
    out = series.astype("string")
    return out.str.strip()

def _ensure_dirs():
    PROJ_DIR.mkdir(parents=True, exist_ok=True)
    SUM_DIR.mkdir(parents=True, exist_ok=True)

def _read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")
    return pd.read_csv(path, **kwargs)

def _require_cols(df: pd.DataFrame, cols: list, name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{name} missing required columns: {missing}")

def _build_mega_z_base() -> pd.DataFrame:
    """
    Build the mega-z table from your existing cleaned source.
    This keeps your current behavior intact.
    """
    src = _read_csv(SOURCE_FILE)
    # Whatever your current build logic is, keep it—here we just pass through.
    # If your real logic transforms/aggregates, leave it as-is.
    df = src.copy()

    # Enforce id key as string
    if "player_id" in df.columns:
        df["player_id"] = _to_str(df["player_id"])
    else:
        raise KeyError("pitchers_normalized_cleaned.csv must contain 'player_id'")

    return df

def _neutral_rows(template: pd.DataFrame, starters: pd.DataFrame, missing_ids: list) -> pd.DataFrame:
    """
    Create neutral/default rows for missing player_ids, using the template columns
    from the current mega_z DataFrame. Numeric columns -> 0.0, booleans -> False,
    strings -> NA (except identifiers we explicitly set). If columns like 'team_id'
    exist in template and starters has them, we copy them over as strings.
    """
    if not missing_ids:
        return template.iloc[0:0].copy()

    # Gather any copyable columns from starters that might exist on template
    copyable = [c for c in ["team_id", "home_team_id", "role", "pitch_hand"] if c in template.columns and c in starters.columns]

    # Build a blank frame with same columns
    blank = pd.DataFrame({c: pd.Series(dtype=template[c].dtype) for c in template.columns})

    rows = []
    for pid in missing_ids:
        row = {}
        for c, dtype in template.dtypes.items():
            if c == "player_id":
                row[c] = str(pid)
            elif c in copyable:
                # copy as string if present; else NA
                val = starters.loc[starters["player_id"] == pid, c]
                row[c] = str(val.iloc[0]) if len(val) > 0 and pd.notna(val.iloc[0]) else pd.NA
            else:
                if pd.api.types.is_float_dtype(dtype) or pd.api.types.is_integer_dtype(dtype):
                    row[c] = 0.0
                elif pd.api.types.is_bool_dtype(dtype):
                    row[c] = False
                else:
                    row[c] = pd.NA
        rows.append(row)

    new_df = pd.DataFrame(rows, columns=template.columns)

    # Ensure string IDs stay strings
    if "player_id" in new_df.columns:
        new_df["player_id"] = _to_str(new_df["player_id"])
    if "team_id" in new_df.columns:
        new_df["team_id"] = _to_str(new_df["team_id"])

    return new_df

def main():
    _ensure_dirs()

    # 1) Build current mega_z from your source (unchanged behavior)
    mega_z = _build_mega_z_base()

    # 2) Load today's starters (must exist because later steps depend on it)
    starters = _read_csv(TODAYS_FILE)
    # Normalize column names and ensure IDs are strings
    starters.columns = [c.strip() for c in starters.columns]
    _require_cols(starters, ["player_id"], "startingpitchers_with_opp_context.csv")
    starters["player_id"] = _to_str(starters["player_id"])
    if "team_id" in starters.columns:
        starters["team_id"] = _to_str(starters["team_id"])

    # 3) Identify missing starters (by player_id)
    mega_ids = set(mega_z["player_id"].astype(str))
    start_ids = set(starters["player_id"].astype(str))
    missing = sorted(list(start_ids - mega_ids))

    # 4) Build neutral rows for any missing starters and append
    if missing:
        add_df = _neutral_rows(mega_z, starters, missing)
        mega_z = pd.concat([mega_z, add_df], ignore_index=True)
        # keep one row per player_id (in case of dupes)
        mega_z = mega_z.drop_duplicates(subset=["player_id"], keep="first").reset_index(drop=True)

        # Write detailed summary files for the guard & debugging
        # Missing list
        pd.DataFrame({"player_id": missing}).to_csv(MISS_FILE, index=False)

        # Coverage map (optional but handy)
        cover = pd.DataFrame({
            "player_id": list(start_ids),
            "present_in_mega_z": [pid in mega_ids for pid in start_ids]
        }).sort_values(["present_in_mega_z","player_id"], ascending=[True, True])
        cover.to_csv(COVER_FILE, index=False)
    else:
        # Ensure files exist (empty) so downstream steps don't break
        pd.DataFrame(columns=["player_id"]).to_csv(MISS_FILE, index=False)
        pd.DataFrame(columns=["player_id","present_in_mega_z"]).to_csv(COVER_FILE, index=False)

    # 5) Enforce final dtypes for identifiers (strings)
    mega_z["player_id"] = _to_str(mega_z["player_id"])
    if "team_id" in mega_z.columns:
        mega_z["team_id"] = _to_str(mega_z["team_id"])

    # 6) Write output
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    mega_z.to_csv(OUT_FILE, index=False)

    # Log summary to STDOUT for your action logs
    print(f"✅ Wrote: {OUT_FILE}  (rows={len(mega_z)})  source={SOURCE_FILE}")
    if missing:
        print(f"ℹ️  Added {len(missing)} missing starter(s) from {TODAYS_FILE}: {', '.join(missing)}")
        print(f"   Missing list: {MISS_FILE}")
        print(f"   Coverage map: {COVER_FILE}")
    else:
        print("ℹ️  All starters already present in mega_z; no additions required.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
