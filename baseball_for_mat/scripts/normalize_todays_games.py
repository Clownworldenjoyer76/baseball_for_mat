#!/usr/bin/env python3
# Purpose: Normalize todaysgames to canonical team abbreviations and attach MLB numeric IDs.
# Inputs:
#   - data/raw/todaysgames.csv
#   - data/manual/team_directory.csv  (headers: team_id, team_code, canonical_team, team_name, clean_team_name, all_codes, all_names)
# Output:
#   - data/raw/todaysgames_normalized.csv  (adds home_team_id, away_team_id; ensures abbreviations)
#
# Mobile-safe, no external deps beyond pandas.

import pandas as pd
from pathlib import Path
import sys

INPUT   = Path("data/raw/todaysgames.csv")
OUTPUT  = Path("data/raw/todaysgames_normalized.csv")
TEAMDIR = Path("data/manual/team_directory.csv")

# ---- utilities ----

def _die(msg: str):
    print(f"INSUFFICIENT INFORMATION\n{msg}")
    sys.exit(1)

def _to_int64(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def _load_games() -> pd.DataFrame:
    if not INPUT.exists():
        _die(f"Missing file: {INPUT}")
    g = pd.read_csv(INPUT, dtype=str).fillna("")
    required = {"home_team", "away_team"}
    if not required.issubset(set(g.columns)):
        _die(f"{INPUT} must include columns: {', '.join(sorted(required))}")
    return g

def _load_teamdir() -> pd.DataFrame:
    if not TEAMDIR.exists():
        _die(f"Missing file: {TEAMDIR}")
    td = pd.read_csv(TEAMDIR, dtype=str).fillna("")
    required = {"team_id","team_code","canonical_team","team_name","clean_team_name","all_codes","all_names"}
    if not required.issubset(td.columns):
        _die(f"{TEAMDIR} must include columns: {', '.join(sorted(required))}")
    return td

def _build_alias_maps(td: pd.DataFrame):
    """
    Returns:
      alias_to_abbr: dict[str->str]  (uppercased key -> team_code)
      abbr_to_id:    dict[str->Int]  (team_code -> team_id int)
    """
    alias_to_abbr = {}
    abbr_to_id = {}

    def put_alias(alias: str, code: str):
        k = (alias or "").strip().upper()
        v = (code or "").strip().upper()
        if k and v and k not in alias_to_abbr:
            alias_to_abbr[k] = v

    for _, r in td.iterrows():
        code = (r.get("team_code","") or "").strip().upper()
        tid  = r.get("team_id","")
        if code:
            abbr_to_id[code] = pd.to_numeric(tid, errors="coerce")
        # primary names
        for col in ("team_code","canonical_team","team_name","clean_team_name"):
            put_alias(r.get(col, ""), code)
        # explode all_names (pipe-delimited)
        for name in (r.get("all_names","") or "").split("|"):
            put_alias(name, code)
        # explode all_codes as well (treat codes as aliases of themselves)
        for ac in (r.get("all_codes","") or "").split("|"):
            put_alias(ac, code)

    return alias_to_abbr, abbr_to_id

def _normalize_team(value: str, alias_to_abbr: dict) -> str:
    key = (value or "").strip().upper()
    if not key:
        return ""
    return alias_to_abbr.get(key, key)  # if already an abbr in map, returns itself; else pass through

# ---- main ----

def normalize():
    games = _load_games()
    td = _load_teamdir()
    alias_to_abbr, abbr_to_id = _build_alias_maps(td)

    # normalize abbreviations
    home_abbr = games["home_team"].map(lambda x: _normalize_team(x, alias_to_abbr))
    away_abbr = games["away_team"].map(lambda x: _normalize_team(x, alias_to_abbr))

    # attach numeric IDs via abbreviation -> team_id
    home_id = home_abbr.map(lambda c: abbr_to_id.get((c or "").strip().upper()))
    away_id = away_abbr.map(lambda c: abbr_to_id.get((c or "").strip().upper()))

    out = games.copy()
    out["home_team"] = home_abbr
    out["away_team"] = away_abbr
    out["home_team_id"] = _to_int64(pd.Series(home_id))
    out["away_team_id"] = _to_int64(pd.Series(away_id))

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT, index=False)
    print(f"✅ normalize_todays_games wrote {len(out)} rows -> {OUTPUT}")

if __name__ == "__main__":
    normalize()
