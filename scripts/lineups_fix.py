#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/lineups_fix.py

Post-process data/raw/lineups.csv to inject:
- player_id          (primary: Data/batters.csv; fallback: team_csvs/batters_*.csv;
                      then primary: Data/pitchers.csv; fallback: team_csvs/pitchers_*.csv)
- type               ("batter" if found in batters, "pitcher" if found in pitchers)
- team_id            (from manual/team_directory.csv via team_code; fallback to 'all_codes')
"""

from __future__ import annotations
import re
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

# --- File paths ---
LINEUPS_PATH = Path("data/raw/lineups.csv")
BATTERS_PATH = Path("data/Data/batters.csv")          # columns: "last_name, first_name", "player_id"
PITCHERS_PATH = Path("data/Data/pitchers.csv")        # columns: "last_name, first_name", "player_id"
TEAM_BATTERS_GLOB = "data/team_csvs/batters_*.csv"    # fallback set
TEAM_PITCHERS_GLOB = "data/team_csvs/pitchers_*.csv"  # fallback set
TEAMS_PATH   = Path("data/manual/team_directory.csv") # "team_code","team_id" and optional "all_codes"

# --- Expected columns in lineups.csv ---
REQUIRED_LINEUPS_COLS = ["team_code", "last_name, first_name", "type", "player_id", "team_id"]


# ------------- helpers -------------
def _strip(s: str) -> str:
    return (s or "").strip()

def _norm_name(s: str) -> str:
    """
    Normalize 'last_name, first_name' for robust matching:
    - Unicode NFKD, strip diacritics
    - lowercase
    - collapse spaces
    - standardize comma spacing to ', '
    """
    s = _strip(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _norm_code(s: str) -> str:
    """Normalize team codes for matching in manual directory."""
    return _strip(s).upper()


# ------------- load lookups -------------
def _mapping_from_df(df: pd.DataFrame) -> Dict[str, str]:
    """
    Build name->player_id from a DataFrame with columns:
    - 'last_name, first_name'
    - 'player_id'
    """
    m: Dict[str, str] = {}
    if "last_name, first_name" not in df.columns or "player_id" not in df.columns:
        return m
    for _, row in df.iterrows():
        name = _norm_name(row["last_name, first_name"])
        pid  = _strip(str(row["player_id"]))
        if name and pid and name not in m:
            m[name] = pid
    return m

def load_name_to_id_map(csv_path: Path) -> Dict[str, str]:
    """Single CSV → mapping."""
    if not csv_path.exists():
        return {}
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    return _mapping_from_df(df)

def load_name_to_id_map_many(paths: Iterable[Path]) -> Dict[str, str]:
    """
    Multiple CSVs (e.g., globbed team files) → merged mapping.
    First non-empty assignment wins to preserve precedence ordering in our caller.
    """
    merged: Dict[str, str] = {}
    for p in paths:
        try:
            df = pd.read_csv(p, dtype=str, keep_default_na=False)
        except Exception:
            continue
        part = _mapping_from_df(df)
        for k, v in part.items():
            if k not in merged:
                merged[k] = v
    return merged

def load_team_maps(csv_path: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns:
      - direct_map: {team_code -> team_id}
      - alias_map : {alias_code -> team_id} from 'all_codes' (if present)
    """
    direct_map: Dict[str, str] = {}
    alias_map : Dict[str, str] = {}

    if not csv_path.exists():
        return direct_map, alias_map

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    if "team_code" in df.columns and "team_id" in df.columns:
        for _, row in df.iterrows():
            code = _norm_code(row["team_code"])
            tid  = _strip(str(row["team_id"]))
            if code and tid:
                direct_map[code] = tid

    if "all_codes" in df.columns and "team_id" in df.columns:
        for _, row in df.iterrows():
            tid = _strip(str(row["team_id"]))
            ac  = _strip(row["all_codes"])
            if not tid or not ac:
                continue
            parts = [p for p in re.split(r"[,\|/\s]+", ac) if p]
            for p in parts:
                alias_map[_norm_code(p)] = tid

    return direct_map, alias_map


# ------------- main logic -------------
def main(
    lineups_path: Path = LINEUPS_PATH,
    batters_path: Path = BATTERS_PATH,
    pitchers_path: Path = PITCHERS_PATH,
    teams_path: Path = TEAMS_PATH,
) -> None:

    # Load lineups
    if not lineups_path.exists():
        raise FileNotFoundError(f"{lineups_path} not found")

    df = pd.read_csv(lineups_path, dtype=str, keep_default_na=False)

    # Ensure required columns exist (create blanks if missing)
    for col in REQUIRED_LINEUPS_COLS:
        if col not in df.columns:
            df[col] = ""

    # ---- Build name→id maps with precedence and fallbacks ----
    # Precedence order we want (first hit wins):
    #   1) Data/batters.csv
    #   2) data/team_csvs/batters_*.csv
    #   3) Data/pitchers.csv
    #   4) data/team_csvs/pitchers_*.csv
    bat_primary   = load_name_to_id_map(batters_path)
    bat_fallbacks = load_name_to_id_map_many(sorted(Path().glob(TEAM_BATTERS_GLOB)))
    pit_primary   = load_name_to_id_map(pitchers_path)
    pit_fallbacks = load_name_to_id_map_many(sorted(Path().glob(TEAM_PITCHERS_GLOB)))

    # Combine into ordered lookup stages
    stages = [
        ("batter", bat_primary),
        ("batter", bat_fallbacks),
        ("pitcher", pit_primary),
        ("pitcher", pit_fallbacks),
    ]

    # Team maps
    direct_map, alias_map = load_team_maps(teams_path)

    # Inject player_id and type
    def _resolve_player(row) -> Tuple[str, str]:
        name = _norm_name(row["last_name, first_name"])
        if not name:
            return "", ""
        for role, mapping in stages:
            pid = mapping.get(name, "")
            if pid:
                return pid, role
        return "", ""  # not found anywhere

    pid_type = df.apply(_resolve_player, axis=1, result_type="expand")
    df["player_id"] = pid_type[0]
    df["type"]      = pid_type[1]

    # Inject team_id (direct code first, then aliases)
    def _resolve_team_id(row) -> str:
        code = _norm_code(row["team_code"])
        if code in direct_map:
            return direct_map[code]
        if code in alias_map:
            return alias_map[code]
        return ""

    df["team_id"] = df.apply(_resolve_team_id, axis=1)

    # Reorder columns to expected order (keep any extras at the end)
    ordered = [c for c in REQUIRED_LINEUPS_COLS if c in df.columns]
    extras  = [c for c in df.columns if c not in ordered]
    df = df[ordered + extras]

    # Overwrite the original lineups CSV
    lineups_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(lineups_path, index=False)


if __name__ == "__main__":
    main()
