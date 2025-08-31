#!/usr/bin/env python3
"""
scripts/apply_team_name_mapping.py

Purpose
-------
Unify team names/abbreviations in:
  - data/raw/todaysgames_normalized.csv
  - data/Data/stadium_metadata.csv

Source of truth
---------------
data/manual/team_directory.csv

Header-agnostic detection (any of the following are accepted):
- Team ID:        team_id, id, mlb_id, teamid
- Abbreviation:   abbr, abbreviation, code, team_abbr, team_code
- Canonical Name: name, team_name, full_name, club_name

Any additional columns (e.g., alias_*, alt_*, nick_*) are treated as aliases.
All values map to the canonical (abbr + name) from team_directory.

Output
------
In-place updates of the two CSVs (if present). Missing files are skipped.

Notes
-----
- Case/spacing/punctuation insensitive matching.
- No API calls.
"""

import sys
from pathlib import Path
import pandas as pd
import re

TEAM_DIR = Path("data/manual/team_directory.csv")
TODAY_GAMES = Path("data/raw/todaysgames_normalized.csv")
STADIUM_META = Path("data/Data/stadium_metadata.csv")


def _norm(s) -> str:
    """Lowercase, strip, remove non-alnum for robust matching."""
    if pd.isna(s):
        return ""
    s = str(s)
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _load_team_directory(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing team directory: {path}")

    df = pd.read_csv(path)

    # Normalize column names
    cols = {c: _norm(c) for c in df.columns}
    df.columns = [cols[c] for c in df.columns]

    # Identify required columns (header-agnostic)
    # team_id
    id_candidates = [c for c in df.columns if c in {"teamid", "id", "mlbid"}]
    # abbreviation
    abbr_candidates = [c for c in df.columns if c in {"abbr", "abbreviation", "code", "teamabbr", "teamcode"}]
    # name
    name_candidates = [c for c in df.columns if c in {"name", "teamname", "fullname", "clubname"}]

    if not id_candidates or not abbr_candidates or not name_candidates:
        raise ValueError(
            "team_directory.csv must include columns for team id, abbreviation, and name "
            "(header-agnostic; examples: team_id/id/mlb_id, abbreviation/code, name/team_name)."
        )

    cid = id_candidates[0]
    cab = abbr_candidates[0]
    cnm = name_candidates[0]

    # Build alias columns list (anything not the 3 core cols)
    core = {cid, cab, cnm}
    alias_cols = [c for c in df.columns if c not in core]

    # Ensure id is int (no decimals)
    df[cid] = pd.to_numeric(df[cid], errors="coerce").astype("Int64")

    # Build mapping dictionaries
    records = []
    for _, r in df.iterrows():
        team_id = r[cid]
        abbr = str(r[cab]).strip() if not pd.isna(r[cab]) else ""
        name = str(r[cnm]).strip() if not pd.isna(r[cnm]) else ""

        keys = set()
        # primary keys
        keys.add(_norm(abbr))
        keys.add(_norm(name))
        # common variants
        keys.add(_norm(name.replace("White Sox", "Whitesox")))
        keys.add(_norm(name.replace("Red Sox", "Redsox")))
        keys.add(_norm(abbr.replace(".", "")))
        # aliases
        for ac in alias_cols:
            v = r.get(ac)
            if pd.notna(v) and str(v).strip():
                keys.add(_norm(v))

        for k in list(keys):
            if k:  # non-empty
                records.append(
                    {
                        "key": k,
                        "team_id": int(team_id) if pd.notna(team_id) else None,
                        "abbr": abbr,
                        "name": name,
                    }
                )

    map_df = pd.DataFrame.from_records(records).drop_duplicates("key")
    if map_df.empty:
        raise ValueError("team_directory.csv produced no usable alias keys.")

    return map_df[["key", "team_id", "abbr", "name"]]


def _canonicalize_column(series: pd.Series, dir_map: pd.DataFrame, return_abbr: bool = True) -> pd.Series:
    """Map free-form team values to canonical abbr (default) or name."""
    lookup = dir_map.set_index("key")[["abbr", "name", "team_id"]]
    out_vals = []
    for v in series.fillna(""):
        k = _norm(v)
        if k in lookup.index:
            out_vals.append(lookup.loc[k, "abbr"] if return_abbr else lookup.loc[k, "name"])
        else:
            out_vals.append(v)  # leave as-is if unknown
    return pd.Series(out_vals, index=series.index)


def _attach_ids(series: pd.Series, dir_map: pd.DataFrame) -> pd.Series:
    lookup = dir_map.set_index("key")["team_id"]
    out = []
    for v in series.fillna(""):
        k = _norm(v)
        out.append(lookup.get(k, pd.NA))
    return pd.Series(out, index=series.index, dtype="Int64")


def _process_todaysgames(dir_map: pd.DataFrame) -> None:
    if not TODAY_GAMES.exists():
        return
    df = pd.read_csv(TODAY_GAMES)

    cols = {c.lower().strip(): c for c in df.columns}
    ht = cols.get("home_team")
    at = cols.get("away_team")

    if ht:
        # canonicalize to ABBR in-place
        df[ht] = _canonicalize_column(df[ht], dir_map, return_abbr=True)
        # also attach ids (new columns if not present)
        if "home_team_id" not in df.columns:
            df["home_team_id"] = _attach_ids(df[ht], dir_map)
    if at:
        df[at] = _canonicalize_column(df[at], dir_map, return_abbr=True)
        if "away_team_id" not in df.columns:
            df["away_team_id"] = _attach_ids(df[at], dir_map)

    df.to_csv(TODAY_GAMES, index=False)


def _process_stadium_meta(dir_map: pd.DataFrame) -> None:
    if not STADIUM_META.exists():
        return
    df = pd.read_csv(STADIUM_META)

    # Try to find a team column to canonicalize (home_team preferred)
    lower = {c.lower().strip(): c for c in df.columns}
    team_like = None
    for key in ("home_team", "team", "club", "franchise"):
        if key in lower:
            team_like = lower[key]
            break

    if team_like:
        # Canonicalize to ABBR for consistency with todaysgames
        df[team_like] = _canonicalize_column(df[team_like], dir_map, return_abbr=True)
        # Attach ID if missing
        if "home_team_id" not in df.columns:
            df["home_team_id"] = _attach_ids(df[team_like], dir_map)

    df.to_csv(STADIUM_META, index=False)


def main():
    try:
        dir_map = _load_team_directory(TEAM_DIR)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    _process_todaysgames(dir_map)
    _process_stadium_meta(dir_map)
    print("Updated: data/raw/todaysgames_normalized.csv (if present)")
    print("Updated: data/Data/stadium_metadata.csv (if present)")


if __name__ == "__main__":
    main()
