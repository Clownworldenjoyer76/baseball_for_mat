#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/lineups_fix.py

Post-process data/raw/lineups.csv to inject:
- player_id          (from Data/batters.csv, else Data/pitchers.csv)
- type               ("batter" if found in batters, "pitcher" if found in pitchers)
- team_id            (from manual/team_directory.csv via team_code; fallback to all_codes)
"""

from __future__ import annotations
import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict

import pandas as pd

# --- File paths ---
LINEUPS_PATH = Path("data/raw/lineups.csv")
BATTERS_PATH = Path("data/Data/batters.csv")      # must have: "last_name, first_name", "player_id"
PITCHERS_PATH = Path("data/Data/pitchers.csv")    # must have: "last_name, first_name", "player_id"
TEAMS_PATH   = Path("data/manual/team_directory.csv")  # must have: "team_code", "team_id"; optional "all_codes"

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
    # normalize unicode/diacritics
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # normalize comma spacing and collapse whitespace
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _norm_code(s: str) -> str:
    """Normalize team codes for matching in manual directory."""
    return _strip(s).upper()


# ------------- load lookups -------------
def load_name_to_id_map(csv_path: Path) -> Dict[str, str]:
    """
    Build a dict: normalized 'last_name, first_name' -> player_id
    from the given CSV (batters or pitchers).
    """
    mapping: Dict[str, str] = {}
    if not csv_path.exists():
        return mapping

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    if "last_name, first_name" not in df.columns or "player_id" not in df.columns:
        return mapping

    for _, row in df.iterrows():
        name = _norm_name(row["last_name, first_name"])
        pid = _strip(str(row["player_id"]))
        if name and pid:
            mapping[name] = pid
    return mapping


def load_team_maps(csv_path: Path) -> tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns:
      - direct_map: {team_code -> team_id}
      - alias_map : {alias_code -> team_id} from 'all_codes' (if present)
        'all_codes' can be a delimited list (commas, pipes, slashes, spaces).
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
            # split on commas, pipes, slashes, or whitespace
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

    # Build lookups
    bat_map = load_name_to_id_map(batters_path)   # name -> player_id
    pit_map = load_name_to_id_map(pitchers_path)  # name -> player_id
    direct_map, alias_map = load_team_maps(teams_path)

    # Inject player_id and type
    def _resolve_player(row) -> tuple[str, str]:
        name = _norm_name(row["last_name, first_name"])
        if name in bat_map:
            return bat_map[name], "batter"
        if name in pit_map:
            return pit_map[name], "pitcher"
        return "", ""  # not found

    pid_type = df.apply(_resolve_player, axis=1, result_type="expand")
    df["player_id"] = pid_type[0]
    df["type"]      = pid_type[1]

    # Inject team_id
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
