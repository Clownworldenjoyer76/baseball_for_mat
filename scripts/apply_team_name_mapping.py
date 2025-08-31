#!/usr/bin/env python3
# Mobile-safe: short, robust, header-agnostic mapping to canonical team_code
import sys
from pathlib import Path
import pandas as pd

TEAM_DIR = Path("data/manual/team_directory.csv")
GAMES_CSV = Path("data/raw/todaysgames_normalized.csv")
STADIUM_CSV = Path("data/Data/stadium_metadata.csv")

def _load_team_directory() -> pd.DataFrame:
    if not TEAM_DIR.exists():
        print(f"ERROR: missing {TEAM_DIR}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(TEAM_DIR, dtype=str).fillna("")
    # Enforce exact headers per spec
    required = {"team_id","team_code","canonical_team","team_name","clean_team_name","all_codes","all_names"}
    have = set(df.columns)
    if not required.issubset(have):
        print("ERROR: team_directory.csv must contain exact headers: "
              "team_id, team_code, canonical_team, team_name, clean_team_name, all_codes, all_names", file=sys.stderr)
        sys.exit(1)
    # Normalize basic fields
    df["team_id"] = df["team_id"].astype(str).str.strip()
    df["team_code"] = df["team_code"].astype(str).str.strip()
    df["canonical_team"] = df["canonical_team"].astype(str).str.strip()
    df["team_name"] = df["team_name"].astype(str).str.strip()
    df["clean_team_name"] = df["clean_team_name"].astype(str).str.strip()
    df["all_codes"] = df["all_codes"].astype(str).str.strip()
    df["all_names"] = df["all_names"].astype(str).str.strip()
    return df

def _build_alias_map(df: pd.DataFrame) -> dict:
    """
    Returns dict where ANY alias (uppercased) -> team_code.
    Covers: team_code, canonical_team, team_name, clean_team_name,
    plus tokens from all_codes and all_names (split by | or ,).
    """
    m = {}
    def add(key, code):
        k = (key or "").strip().upper()
        c = (code or "").strip()
        if k and c and k not in m:
            m[k] = c

    for _, r in df.iterrows():
        code = r["team_code"]
        add(r["team_code"], code)
        add(r["canonical_team"], code)
        add(r["team_name"], code)
        add(r["clean_team_name"], code)

        for bucket in ("all_codes","all_names"):
            raw = r[bucket]
            if raw:
                # split on | or , and spaces
                parts = [p for token in raw.split("|") for p in token.split(",")]
                for p in parts:
                    add(p, code)
    return m

def _normalize_col(series: pd.Series, alias_map: dict) -> pd.Series:
    return series.astype(str).apply(lambda v: alias_map.get(v.strip().upper(), v.strip()))

def _update_games(alias_map: dict):
    if not GAMES_CSV.exists():
        return
    g = pd.read_csv(GAMES_CSV)
    # Only touch known columns; leave others intact
    for col in ("home_team","away_team"):
        if col in g.columns:
            g[col] = _normalize_col(g[col], alias_map)
    g.to_csv(GAMES_CSV, index=False)
    print(f"Updated: {GAMES_CSV} (if present)")

def _update_stadium(alias_map: dict, team_dir: pd.DataFrame):
    if not STADIUM_CSV.exists():
        return
    s = pd.read_csv(STADIUM_CSV)
    # Detect a team identifier column commonly used
    team_col = None
    for c in ("team","home_team","Team","HOME_TEAM"):
        if c in s.columns:
            team_col = c
            break
    if team_col is None:
        # No identifiable team column; keep file unchanged
        print(f"No team column found in {STADIUM_CSV}; left unchanged.")
        return

    # Normalize to canonical team_code
    s["team_code"] = _normalize_col(s[team_col], alias_map)

    # Attach team_id if not present
    if "team_id" not in s.columns:
        s = s.merge(team_dir[["team_id","team_code"]], on="team_code", how="left")

    # Standardize identifier column name to 'team'
    s.drop(columns=[team_col], inplace=True)
    s.rename(columns={"team_code":"team"}, inplace=True)

    s.to_csv(STADIUM_CSV, index=False)
    print(f"Updated: {STADIUM_CSV} (if present)")

def main():
    td = _load_team_directory()
    amap = _build_alias_map(td)
    _update_games(amap)
    _update_stadium(amap, td)

if __name__ == "__main__":
    main()
