#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/inject_pitcher_ids_into_games.py

Inject pitcher_home_id and pitcher_away_id into data/raw/todaysgames_normalized.csv
using data/processed/player_team_master.csv first, then falling back to team
rosters in data/team_csvs/*.csv.

Strict rules:
- No assumptions beyond provided files.
- Match by name only (games file provides names); do robust normalization.
- Preserve ALL existing columns; only update/insert pitcher_home_id, pitcher_away_id.
- Accept 'Undecided' => leave ID blank.
- Write IDs as plain digit strings (no '.0').
"""

import pandas as pd
from pathlib import Path
from unidecode import unidecode
import re
import glob

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
ROSTERS_DIR = Path("data/team_csvs")  # files like pitchers_Tigers.csv, pitchers_Twins.csv

# --------------------------
# Helpers
# --------------------------
def clean_spaces(s: str) -> str:
    s = re.sub(r"\s+", " ", s.strip())
    return s

def normalize_person_name(name: str) -> str:
    """
    Return a canonical 'Last, First Middle' (title case), accent-folded, commas kept.
    Handles inputs in either 'Last, First [Middle]' or 'First [Middle] Last'.
    """
    if name is None:
        return ""
    s = str(name).strip()
    if s == "" or s.lower() == "undecided":
        return ""

    # fold accents, remove extra punctuation noise but keep comma
    s = unidecode(s)
    s = s.replace(".", " ")
    s = clean_spaces(s)

    if "," in s:
        # assume already "Last, First [Middle]"
        parts = [p.strip() for p in s.split(",", 1)]
        last = parts[0]
        firstmid = clean_spaces(parts[1]) if len(parts) > 1 else ""
        canon = f"{last.title()}, {firstmid.title()}".strip().rstrip(",")
        return canon

    # assume "First [Middle] Last"
    tokens = s.split(" ")
    if len(tokens) == 1:
        # single token name; return as-is
        return tokens[0].title()
    last = tokens[-1]
    firstmid = " ".join(tokens[:-1])
    canon = f"{last.title()}, {firstmid.title()}"
    canon = clean_spaces(canon)
    return canon

def alt_variants(name: str) -> list:
    """
    Generate conservative alternate variants to improve matching.
    """
    if not name:
        return []
    base = normalize_person_name(name)
    variants = {base}

    # Variant without spaces after comma
    variants.add(base.replace(", ", ","))

    # Swap first/middle order (e.g., 'Richardson, Simeon Woods' <-> 'Richardson, Simeon W')
    if ", " in base:
        last, firstmid = base.split(", ", 1)
        fm_tokens = firstmid.split(" ")
        if len(fm_tokens) >= 2:
            # move middle to end/start variants
            # e.g., Simeon Woods -> Woods Simeon (rarely needed, but harmless)
            variants.add(f"{last}, {' '.join(reversed(fm_tokens))}")

    # Lowercase accentless compact
    compact = unidecode(base).lower().replace(", ", ",").replace(" ", "")
    variants.add(compact)

    return list(variants)

def build_master_index(master_path: Path) -> dict:
    """
    Build name->player_id mapping from player_team_master.csv.
    Requires columns: name, player_id.
    """
    if not master_path.exists():
        raise FileNotFoundError(f"INSUFFICIENT INFORMATION: {master_path} is missing.")

    df = pd.read_csv(master_path)
    required = {"name", "player_id"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {master_path} missing columns: {sorted(missing)}")

    df = df.dropna(subset=["name", "player_id"]).copy()
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["player_id"])
    df["canon"] = df["name"].apply(normalize_person_name)
    df["compact"] = df["canon"].apply(lambda s: unidecode(s).lower().replace(", ", ",").replace(" ", ""))

    index = {}
    for _, r in df.iterrows():
        pid = str(int(r["player_id"]))
        index.setdefault(r["canon"], set()).add(pid)
        index.setdefault(r["compact"], set()).add(pid)
    return index

def build_roster_index(rosters_dir: Path) -> dict:
    """
    Build supplemental name->player_id mapping from data/team_csvs/*.csv
    (e.g., pitchers_Tigers.csv, pitchers_Twins.csv). Must have columns: name, player_id.
    """
    index = {}
    for path in glob.glob(str(rosters_dir / "*.csv")):
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if not {"name", "player_id"}.issubset(df.columns):
            continue
        df = df.dropna(subset=["name", "player_id"]).copy()
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["player_id"])
        df["canon"] = df["name"].apply(normalize_person_name)
        df["compact"] = df["canon"].apply(lambda s: unidecode(s).lower().replace(", ", ",").replace(" ", ""))

        for _, r in df.iterrows():
            pid = str(int(r["player_id"]))
            index.setdefault(r["canon"], set()).add(pid)
            index.setdefault(r["compact"], set()).add(pid)
    return index

def lookup_player_id(name: str, primary_idx: dict, fallback_idx: dict) -> str:
    """
    Try to find a single player_id for the given name using:
      1) normalized forms in primary index
      2) variants
      3) fallback roster index
    If multiple PIDs are found, choose the lowest (deterministic). If none, return "".
    """
    if not name or name.lower() == "undecided":
        return ""

    # Try canonical + compact in primary
    canon = normalize_person_name(name)
    compact = unidecode(canon).lower().replace(", ", ",").replace(" ", "")
    candidates = set()
    for key in (canon, compact):
        if key in primary_idx:
            candidates |= primary_idx[key]
    if candidates:
        return sorted(candidates, key=lambda x: int(x))[0]

    # Try variants in primary
    for v in alt_variants(name):
        if v in primary_idx:
            return sorted(primary_idx[v], key=lambda x: int(x))[0]

    # Fall back to roster index
    for key in (canon, compact, *alt_variants(name)):
        if key in fallback_idx:
            return sorted(fallback_idx[key], key=lambda x: int(x))[0]

    return ""

# --------------------------
# Main
# --------------------------
def main():
    # Load games
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} is missing.")
    games = pd.read_csv(GAMES_FILE)

    required_g_cols = {"game_id", "home_team", "away_team", "pitcher_home", "pitcher_away"}
    missing_g = required_g_cols - set(games.columns)
    if missing_g:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(missing_g)}")

    # Ensure ID columns exist as strings (preserve blanks cleanly)
    for col in ("pitcher_home_id", "pitcher_away_id"):
        if col not in games.columns:
            games[col] = ""
        else:
            # Convert any float-looking values like 622503.0 -> "622503"
            games[col] = pd.to_numeric(games[col], errors="coerce").astype("Int64").astype("string").replace({"<NA>": ""})

    # Build indices
    primary_idx = build_master_index(MASTER_FILE)
    roster_idx = build_roster_index(ROSTERS_DIR)

    # Inject IDs
    def inject(existing: str, name: str) -> str:
        # keep existing if already a valid integer string
        if existing and existing.isdigit():
            return existing
        pid = lookup_player_id(name, primary_idx, roster_idx)
        return pid

    games["pitcher_home_id"] = [
        inject(str(games.at[i, "pitcher_home_id"]), games.at[i, "pitcher_home"])
        for i in range(len(games))
    ]
    games["pitcher_away_id"] = [
        inject(str(games.at[i, "pitcher_away_id"]), games.at[i, "pitcher_away"])
        for i in range(len(games))
    ]

    # Preserve all columns; only update the two ID fields
    out_cols = list(games.columns)
    games[out_cols].to_csv(GAMES_FILE, index=False)

    # Quick summary
    h_ok = sum(bool(x) for x in games["pitcher_home_id"])
    a_ok = sum(bool(x) for x in games["pitcher_away_id"])
    total = len(games)
    print(f"✅ Injected pitcher IDs -> {GAMES_FILE} (home_id non-null: {h_ok}/{total}, away_id non-null: {a_ok}/{total})")

if __name__ == "__main__":
    main()
