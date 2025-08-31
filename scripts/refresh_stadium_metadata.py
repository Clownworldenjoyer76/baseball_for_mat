#!/usr/bin/env python3
"""
Builds data/Data/stadium_metadata.csv from manual inputs.

Reads (preferred locations, in order):
  - data/manual/mlb_team_ids.csv
  - data/mlb_team_ids.csv
  - data/Data/mlb_team_ids.csv

Also uses (under data/manual):
  - park_factors_day.csv
  - park_factors_night.csv
  - park_factors_roof_closed.csv

Optional filter to today's home teams if data/raw/todaysgames_normalized.csv exists.

Output:
  - data/Data/stadium_metadata.csv
"""
from pathlib import Path
import pandas as pd
import re

PREF_TEAM_ID_PATHS = [
    Path("data/manual/mlb_team_ids.csv"),
    Path("data/mlb_team_ids.csv"),
    Path("data/Data/mlb_team_ids.csv"),
]
MANUAL_DIR = Path("data/manual")
OUT_PATH   = Path("data/Data/stadium_metadata.csv")
TODAYS     = Path("data/raw/todaysgames_normalized.csv")

# ----------------- helpers -----------------
def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    def norm(c: str) -> str:
        c = c.strip().lower()
        c = re.sub(r"[^a-z0-9]+", "_", c)
        return c.strip("_")
    df.columns = [norm(c) for c in df.columns]
    return df

def _pick_col(df: pd.DataFrame, want: str) -> str | None:
    """
    want: 'name' | 'id' | 'abbr'
    Extremely permissive selection.
    """
    cols = list(df.columns)

    # exact favorites
    favorites = {
        "name": ["team_name", "name", "team", "club_name", "full_name", "fullname"],
        "id":   ["team_id", "id", "mlb_team_id", "home_team_id"],
        "abbr": ["abbreviation", "abbr", "team_abbr", "team_code", "code"],
    }[want]
    for c in favorites:
        if c in df.columns:
            return c

    # heuristic fallback
    for c in cols:
        cl = c.lower()
        if want == "name" and ("name" in cl or "team" in cl):
            return c
        if want == "id" and "id" in cl:
            return c
        if want == "abbr" and any(k in cl for k in ["abbr", "abbreviation", "code"]):
            return c
    return None

def _coerce_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def _read_csv_any_header(path: Path) -> pd.DataFrame:
    """
    Try normal read; if columns still unusable, try header=None expecting 3 columns.
    """
    df = pd.read_csv(path)
    if df.shape[1] >= 3:
        return df
    # fallback: attempt no-header
    df = pd.read_csv(path, header=None)
    return df

def _load_team_ids() -> pd.DataFrame:
    src = next((p for p in PREF_TEAM_ID_PATHS if p.exists()), None)
    if not src:
        raise FileNotFoundError("mlb_team_ids.csv not found in data/manual/, data/, or data/Data/")

    df = _read_csv_any_header(src)
    df = _norm_cols(df)

    # If no useful header, rename first three to generic
    if df.shape[1] >= 3 and set(df.columns) == {0,1,2}:
        df = df.rename(columns={0: "team_name", 1: "team_id", 2: "abbreviation"})

    name_col = _pick_col(df, "name")
    id_col   = _pick_col(df, "id")
    abbr_col = _pick_col(df, "abbr")

    if not (name_col and id_col and abbr_col):
        cols_str = ", ".join(df.columns)
        raise ValueError(
            f"mlb_team_ids.csv missing required columns (name/id/abbreviation). "
            f"Found columns: [{cols_str}] at {src}"
        )

    out = pd.DataFrame({
        "team_id": _coerce_int(df[id_col]),
        "abbr": df[abbr_col].astype(str).str.strip(),
        "team_name": df[name_col].astype(str).str.strip(),
    }).dropna(subset=["team_id"]).drop_duplicates(subset=["team_id"])

    # Normalize specific project codes
    out["abbr"] = out["abbr"].replace({"OAK": "ATH", "CWS": "CHW"})

    return out

def _load_pf(fname: str, value_col_name: str) -> pd.DataFrame:
    fp = MANUAL_DIR / fname
    if not fp.exists():
        # Return empty DF; merges will just yield NaN
        return pd.DataFrame(columns=["team_id", value_col_name])
    df = pd.read_csv(fp)
    df = _norm_cols(df)

    id_col = _pick_col(df, "id")
    if not id_col:
        cols_str = ", ".join(df.columns)
        raise ValueError(f"{fname} missing team id column. Found columns: [{cols_str}]")

    # choose factor column
    candidates = [c for c in df.columns if c != id_col]
    prefer = [c for c in candidates if ("factor" in c or c.endswith("_pf") or c == "pf")]
    col_factor = prefer[0] if prefer else (candidates[0] if candidates else None)
    if not col_factor:
        cols_str = ", ".join(df.columns)
        raise ValueError(f"{fname} missing park factor value column. Found columns: [{cols_str}]")

    out = pd.DataFrame({
        "team_id": _coerce_int(df[id_col]),
        value_col_name: pd.to_numeric(df[col_factor], errors="coerce")
    }).dropna(subset=["team_id"]).drop_duplicates(subset=["team_id"])

    return out

def _load_todays_home_abbrs() -> set:
    if not TODAYS.exists():
        return set()
    df = pd.read_csv(TODAYS)
    df = _norm_cols(df)
    col_home = "home_team" if "home_team" in df.columns else None
    if not col_home:
        return set()
    return set(df[col_home].astype(str).str.strip().unique())

# ----------------- main -----------------
def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    teams = _load_team_ids()
    pf_day   = _load_pf("park_factors_day.csv", "park_factor_day")
    pf_night = _load_pf("park_factors_night.csv", "park_factor_night")
    pf_roof  = _load_pf("park_factors_roof_closed.csv", "park_factor_roof_closed")

    meta = teams.merge(pf_day,   on="team_id", how="left") \
                .merge(pf_night, on="team_id", how="left") \
                .merge(pf_roof,  on="team_id", how="left")

    todays_home = _load_todays_home_abbrs()
    if todays_home:
        meta = meta[ meta["abbr"].isin(todays_home) ].copy()

    meta = meta.sort_values(["abbr"]).reset_index(drop=True)
    cols = ["team_id","abbr","team_name","park_factor_day","park_factor_night","park_factor_roof_closed"]
    for c in cols:
        if c not in meta.columns:
            meta[c] = pd.NA
    meta = meta[cols]

    meta.to_csv(OUT_PATH, index=False)
    print(f"Saved updated stadium metadata to {OUT_PATH} (rows={len(meta)})")

if __name__ == "__main__":
    main()
