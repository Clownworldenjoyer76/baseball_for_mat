#!/usr/bin/env python3
"""
Normalizes team abbreviations in:
  - data/raw/todaysgames_normalized.csv
  - data/Data/stadium_metadata.csv (if present)

Uses mlb_team_ids.csv with header-agnostic parsing and multiple search paths.
"""
from pathlib import Path
import pandas as pd
import re

PREF_TEAM_ID_PATHS = [
    Path("data/manual/mlb_team_ids.csv"),
    Path("data/mlb_team_ids.csv"),
    Path("data/Data/mlb_team_ids.csv"),
]
GAMES = Path("data/raw/todaysgames_normalized.csv")
STAD  = Path("data/Data/stadium_metadata.csv")

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    def norm(c: str) -> str:
        c = c.strip().lower()
        c = re.sub(r"[^a-z0-9]+", "_", c)
        return c.strip("_")
    df.columns = [norm(c) for c in df.columns]
    return df

def _pick_col(df: pd.DataFrame, want: str) -> str | None:
    favorites = {
        "name": ["team_name","name","team","club_name","full_name","fullname"],
        "id":   ["team_id","id","mlb_team_id","home_team_id"],
        "abbr": ["abbreviation","abbr","team_abbr","team_code","code"],
    }[want]
    for c in favorites:
        if c in df.columns:
            return c
    for c in df.columns:
        cl = c.lower()
        if want == "name" and ("name" in cl or "team" in cl):
            return c
        if want == "id" and "id" in cl:
            return c
        if want == "abbr" and any(k in cl for k in ["abbr","abbreviation","code"]):
            return c
    return None

def _read_csv_any_header(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.shape[1] >= 3:
        return df
    df = pd.read_csv(path, header=None)
    return df

def _load_team_ids() -> pd.DataFrame:
    src = next((p for p in PREF_TEAM_ID_PATHS if p.exists()), None)
    if not src:
        raise FileNotFoundError("mlb_team_ids.csv not found in data/manual/, data/, or data/Data/")

    df = _read_csv_any_header(src)
    df = _norm_cols(df)

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
        "team_id": pd.to_numeric(df[id_col], errors="coerce").astype("Int64"),
        "abbr": df[abbr_col].astype(str).str.strip(),
        "team_name": df[name_col].astype(str).str.strip(),
    }).dropna(subset=["team_id"]).drop_duplicates(subset=["team_id"])

    out["abbr"] = out["abbr"].replace({"OAK": "ATH", "CWS": "CHW"})

    return out

def _normalize_abbr(s: str) -> str:
    s = (s or "").strip()
    if s in {"OAK", "ATH"}:
        return "ATH"
    if s in {"CWS", "CHW"}:
        return "CHW"
    return s

def _norm_games():
    if not GAMES.exists():
        return
    df = pd.read_csv(GAMES)
    if df.empty:
        return
    df = _norm_cols(df)
    for col in ("home_team", "away_team"):
        if col in df.columns:
            # map on the original, not the normalized copy
            pass
    # re-read and write with mapping to preserve original header casing
    g = pd.read_csv(GAMES)
    for col in ("home_team", "away_team"):
        if col in g.columns:
            g[col] = g[col].astype(str).map(_normalize_abbr)
    g.to_csv(GAMES, index=False)
    print(f"✅ Updated: {GAMES}")

def _touch_stadium_meta():
    if not STAD.exists():
        return
    df = pd.read_csv(STAD)
    if "abbr" in df.columns:
        df["abbr"] = df["abbr"].astype(str).map(_normalize_abbr)
        df.to_csv(STAD, index=False)
        print(f"✅ Updated: {STAD}")

def main():
    _ = _load_team_ids()
    _norm_games()
    _touch_stadium_meta()

if __name__ == "__main__":
    main()
