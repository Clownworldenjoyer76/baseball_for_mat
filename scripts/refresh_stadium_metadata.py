#!/usr/bin/env python3
"""
scripts/refresh_stadium_metadata.py

Purpose
-------
Ensure stadium metadata aligns with canonical MLB team identifiers
using data/manual/team_directory.csv as the single source of truth.

Behavior
--------
- Loads existing data/Data/stadium_metadata.csv (if present).
- Loads data/raw/todaysgames_normalized.csv to determine active home teams.
- Loads data/manual/team_directory.csv (header-agnostic).
- Canonicalizes team labels to team abbreviations and attaches team_id.
- Writes back to data/Data/stadium_metadata.csv.

Notes
-----
- Header-agnostic reading of team_directory.
- No external API calls here.
"""

import sys
from pathlib import Path
import pandas as pd
import re

TEAM_DIR = Path("data/manual/team_directory.csv")
STADIUM_META = Path("data/Data/stadium_metadata.csv")
TODAY_GAMES = Path("data/raw/todaysgames_normalized.csv")


def _norm(s) -> str:
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
    df.columns = [_norm(c) for c in df.columns]

    id_candidates = [c for c in df.columns if c in {"teamid", "id", "mlbid"}]
    abbr_candidates = [c for c in df.columns if c in {"abbr", "abbreviation", "code", "teamabbr", "teamcode"}]
    name_candidates = [c for c in df.columns if c in {"name", "teamname", "fullname", "clubname"}]

    if not id_candidates or not abbr_candidates or not name_candidates:
        raise ValueError(
            "team_directory.csv must include columns for team id, abbreviation, and name "
            "(header-agnostic; examples: team_id/id/mlb_id, abbreviation/code, name/team_name)."
        )

    cid = id_candidates[0]
    cab = abbr_candidates[0]
    cnm = name_candidates[0]

    core = {cid, cab, cnm}
    alias_cols = [c for c in df.columns if c not in core]

    df[cid] = pd.to_numeric(df[cid], errors="coerce").astype("Int64")

    rows = []
    for _, r in df.iterrows():
        tid = r[cid]
        ab = str(r[cab]).strip() if not pd.isna(r[cab]) else ""
        nm = str(r[cnm]).strip() if not pd.isna(r[cnm]) else ""
        keys = {_norm(ab), _norm(nm), _norm(ab.replace(".", ""))}
        keys.add(_norm(nm.replace("White Sox", "Whitesox")))
        keys.add(_norm(nm.replace("Red Sox", "Redsox")))
        for ac in alias_cols:
            v = r.get(ac)
            if pd.notna(v) and str(v).strip():
                keys.add(_norm(v))
        for k in keys:
            if k:
                rows.append({"key": k, "team_id": int(tid) if pd.notna(tid) else None, "abbr": ab, "name": nm})

    m = pd.DataFrame(rows).drop_duplicates("key")
    if m.empty:
        raise ValueError("team_directory.csv produced no usable alias keys.")

    return m[["key", "team_id", "abbr", "name"]]


def _canon(series: pd.Series, dir_map: pd.DataFrame, to_abbr: bool = True) -> pd.Series:
    lk = dir_map.set_index("key")[["abbr", "name", "team_id"]]
    out = []
    for v in series.fillna(""):
        k = _norm(v)
        if k in lk.index:
            out.append(lk.loc[k, "abbr"] if to_abbr else lk.loc[k, "name"])
        else:
            out.append(v)
    return pd.Series(out, index=series.index)


def _attach_id(series: pd.Series, dir_map: pd.DataFrame) -> pd.Series:
    lk = dir_map.set_index("key")["team_id"]
    return pd.Series([lk.get(_norm(v), pd.NA) for v in series.fillna("")], index=series.index, dtype="Int64")


def main():
    # Load team directory (single source of truth)
    try:
        dir_map = _load_team_directory(TEAM_DIR)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # Load stadium metadata if present; otherwise start an empty frame
    if STADIUM_META.exists():
        sm = pd.read_csv(STADIUM_META)
    else:
        sm = pd.DataFrame()

    # If todaysgames exists, ensure we have one row per active home team in stadium metadata
    if TODAY_GAMES.exists():
        tg = pd.read_csv(TODAY_GAMES)
        # find home team column
        lower = {c.lower().strip(): c for c in tg.columns}
        ht = lower.get("home_team")
        if ht:
            homes = tg[[ht]].dropna().drop_duplicates()
            homes.columns = ["home_team"]
            # canonicalize home team to ABBR and attach ID
            homes["home_team"] = _canon(homes["home_team"], dir_map, to_abbr=True)
            homes["home_team_id"] = _attach_id(homes["home_team"], dir_map)

            # Merge into sm (by home_team/home_team_id if present)
            if not sm.empty:
                sm_cols = {c.lower().strip(): c for c in sm.columns}
                sm_team_col = sm_cols.get("home_team") or sm_cols.get("team") or None
                if sm_team_col:
                    sm = sm.copy()
                    sm[sm_team_col] = _canon(sm[sm_team_col], dir_map, to_abbr=True)
                    if "home_team_id" not in sm.columns:
                        sm["home_team_id"] = _attach_id(sm[sm_team_col], dir_map)
                    # ensure all active homes exist
                    key_cols = ["home_team_id"] if "home_team_id" in sm.columns else [sm_team_col]
                    sm = pd.merge(
                        homes, sm, left_on=key_cols, right_on=key_cols, how="left", suffixes=("", "_y")
                    )
                    # drop any duplicated helper columns from merge
                    dup_y = [c for c in sm.columns if c.endswith("_y")]
                    if dup_y:
                        sm = sm.drop(columns=dup_y)
                else:
                    # No recognizable team column: rebuild minimal sheet from homes
                    sm = homes.copy()
            else:
                sm = homes.copy()

    # Always write back if we reached here
    STADIUM_META.parent.mkdir(parents=True, exist_ok=True)
    sm.to_csv(STADIUM_META, index=False)
    print(f"Saved updated stadium metadata to {STADIUM_META}")


if __name__ == "__main__":
    main()
