#!/usr/bin/env python3
"""
Aligns team names/abbreviations in:
  - data/raw/todaysgames_normalized.csv
  - data/Data/stadium_metadata.csv   (if already present)

Uses data/manual/mlb_team_ids.csv (header-agnostic).

Result:
  - Overwrites todaysgames_normalized.csv with normalized abbreviations
  - Leaves stadium_metadata.csv untouched if missing; otherwise ensures 'abbr' is normalized (no hard fail)
"""

from pathlib import Path
import pandas as pd

TEAM_IDS = Path("data/manual/mlb_team_ids.csv")
GAMES    = Path("data/raw/todaysgames_normalized.csv")
STAD     = Path("data/Data/stadium_metadata.csv")

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def _load_team_ids():
    df = pd.read_csv(TEAM_IDS)
    df = _norm_cols(df)
    col_team_name = next((c for c in df.columns if c in {"team_name","name"}), None)
    col_team_id   = next((c for c in df.columns if c in {"team_id","id","mlb_team_id"}), None)
    col_abbr      = next((c for c in df.columns if c in {"abbreviation","abbr","team_abbr","team_code"}), None)
    if not (col_team_name and col_team_id and col_abbr):
        raise ValueError("mlb_team_ids.csv must have name/id/abbreviation columns (any common variant).")
    out = pd.DataFrame({
        "team_id": pd.to_numeric(df[col_team_id], errors="coerce").astype("Int64"),
        "abbr": df[col_abbr].astype(str).str.strip(),
        "team_name": df[col_team_name].astype(str).str.strip(),
    })
    out = out.dropna(subset=["team_id"]).drop_duplicates(subset=["team_id"])
    # Build maps
    abbr_from_name = {r.team_name: r.abbr for _, r in out.iterrows()}
    valid_abbrs = set(out["abbr"])
    return out, abbr_from_name, valid_abbrs

def _normalize_abbr(s: str) -> str:
    s = (s or "").strip()
    # known API quirks: use your established codes
    if s in {"OAK","ATH"}: return "ATH"  # your project uses ATH to identify Athletics
    if s in {"CWS","CHW"}: return "CHW"
    return s

def _norm_games():
    if not GAMES.exists():
        return
    games = pd.read_csv(GAMES)
    g = _norm_cols(games)
    _, _, valid_abbrs = _load_team_ids()

    for col in ["home_team","away_team"]:
        col0 = next((c for c in g.columns if c == col), None)
        if col0:
            games[col] = games[col].astype(str).map(_normalize_abbr)

    # Optional: warn/log any out-of-vocabulary abbrs (no hard stop)
    for col in ["home_team","away_team"]:
        if col in games.columns:
            bad = sorted(set(games[col].astype(str)) - set(map(_normalize_abbr, valid_abbrs)))
            if bad:
                print(f"Warning: unexpected team codes in {col}: {bad}")

    games.to_csv(GAMES, index=False)
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
    _ = _load_team_ids()  # validates headers (header-agnostic)
    _norm_games()
    _touch_stadium_meta()

if __name__ == "__main__":
    main()
