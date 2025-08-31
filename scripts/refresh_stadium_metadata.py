#!/usr/bin/env python3
"""
Builds data/Data/stadium_metadata.csv from data/manual inputs.

Inputs (under data/manual):
  - mlb_team_ids.csv
  - park_factors_day.csv
  - park_factors_night.csv
  - park_factors_roof_closed.csv

Optional (to restrict to teams playing today):
  - data/raw/todaysgames_normalized.csv  (reads home team abbreviations)

Output:
  - data/Data/stadium_metadata.csv
"""

from pathlib import Path
import pandas as pd

MANUAL_DIR = Path("data/manual")
OUT_PATH   = Path("data/Data/stadium_metadata.csv")
TODAYS     = Path("data/raw/todaysgames_normalized.csv")

# ---------- header-agnostic helpers ----------
def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def _load_team_ids() -> pd.DataFrame:
    """
    Accepts flexible headers. We map to:
      team_id (int), abbr (string), team_name (string)
    """
    fp = MANUAL_DIR / "mlb_team_ids.csv"
    df = pd.read_csv(fp)
    df = _norm_cols(df)

    # Known aliases
    col_team_name = next((c for c in df.columns if c in {"team_name","name"}), None)
    col_team_id   = next((c for c in df.columns if c in {"team_id","id","mlb_team_id"}), None)
    col_abbr      = next((c for c in df.columns if c in {"abbreviation","abbr","team_abbr","team_code"}), None)

    if not (col_team_name and col_team_id and col_abbr):
        raise ValueError("mlb_team_ids.csv must contain team name/id/abbreviation columns (header-agnostic).")

    out = pd.DataFrame({
        "team_id": pd.to_numeric(df[col_team_id], errors="coerce").astype("Int64"),
        "abbr": df[col_abbr].astype(str).str.strip(),
        "team_name": df[col_team_name].astype(str).str.strip(),
    })
    out = out.dropna(subset=["team_id"]).drop_duplicates(subset=["team_id"])
    return out

def _load_pf(fname: str, value_col_name: str) -> pd.DataFrame:
    """
    Accepts flexible headers; returns [team_id, value_col_name]
    """
    fp = MANUAL_DIR / fname
    df = pd.read_csv(fp)
    df = _norm_cols(df)

    col_team_id = next((c for c in df.columns if c in {"team_id","id","home_team_id","mlb_team_id"}), None)
    if not col_team_id:
        raise ValueError(f"{fname} must include a team id column.")

    # Any numeric column to serve as the factor if unnamed
    # Prefer obvious names
    cand = [c for c in df.columns if c not in {col_team_id}]
    # Heuristics: look for factor-like columns
    prefer = [c for c in cand if "factor" in c or "pf" in c]
    col_factor = prefer[0] if prefer else (cand[0] if cand else None)
    if not col_factor:
        raise ValueError(f"{fname} must contain a park factor value column.")

    out = pd.DataFrame({
        "team_id": pd.to_numeric(df[col_team_id], errors="coerce").astype("Int64"),
        value_col_name: pd.to_numeric(df[col_factor], errors="coerce")
    })
    out = out.dropna(subset=["team_id"]).drop_duplicates(subset=["team_id"])
    return out

def _load_todays_home_abbrs() -> set:
    """
    Reads todaysgames_normalized if present; returns set of home team abbreviations.
    Header-agnostic: looks for 'home_team' (case-insensitive).
    """
    if not TODAYS.exists():
        return set()
    df = pd.read_csv(TODAYS)
    df = _norm_cols(df)
    col_home = next((c for c in df.columns if c in {"home_team","home"}), None)
    if not col_home:
        return set()
    return set(df[col_home].astype(str).str.strip().unique())

# ---------- main ----------
def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    teams = _load_team_ids()
    pf_day   = _load_pf("park_factors_day.csv", "park_factor_day")
    pf_night = _load_pf("park_factors_night.csv", "park_factor_night")
    pf_roof  = _load_pf("park_factors_roof_closed.csv", "park_factor_roof_closed")

    # Merge
    meta = teams.merge(pf_day,   on="team_id", how="left") \
                .merge(pf_night, on="team_id", how="left") \
                .merge(pf_roof,  on="team_id", how="left")

    # Optional: restrict to today’s home teams if that file exists and matches abbreviations
    todays_home = _load_todays_home_abbrs()
    if todays_home:
        meta = meta[ meta["abbr"].isin(todays_home) ].copy()

    # Sort & write
    meta = meta.sort_values(["abbr"]).reset_index(drop=True)

    # Final column order (stable, compact)
    meta = meta[["team_id","abbr","team_name","park_factor_day","park_factor_night","park_factor_roof_closed"]]

    meta.to_csv(OUT_PATH, index=False)
    print(f"Saved updated stadium metadata to {OUT_PATH} (rows={len(meta)})")

if __name__ == "__main__":
    main()
