#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/inject_pitcher_ids_into_games.py

Robustly inject pitcher IDs into data/raw/todaysgames_normalized.csv

Improvements:
- Normalize names (remove accents, strip punctuation, collapse whitespace, lowercase)
- Generate multiple variants per name (handles "Last, First" <-> "First Last")
- Sources: player_team_master.csv, team_csvs/pitchers_*.csv, startingpitchers_with_opp_context.csv (if present)
- Team-aware last-name fallback when there is exactly one candidate on that team
- Ignore placeholder/negative IDs and try to resolve
- Write unresolved report to summaries/foundation/missing_pitcher_ids.txt
"""

from __future__ import annotations

import csv
import glob
import re
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd

# ---- Paths ----
GAMES_FILE  = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
TEAM_PITCHERS_GLOB = "data/team_csvs/pitchers_*.csv"
SP_LONG_FILE = Path("data/raw/startingpitchers_with_opp_context.csv")

TEAMDIR = Path("data/manual/team_directory.csv")  # only used if we need abbr->id (not required)
SUMMARY_DIR = Path("summaries/foundation")
MISSING_LOG = SUMMARY_DIR / "missing_pitcher_ids.txt"

# Authoritative hand overrides (name string -> player_id)
OVERRIDES: Dict[str, int] = {
    "Richardson, Simeon Woods": 680573,
    "Gipson-Long, Sawyer": 687830,
    "Berríos, José": 621244,
}

# ---- Name normalization helpers ----

_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")

def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )

def norm_basic(s: str) -> str:
    s = s or ""
    s = strip_accents(s)
    s = s.lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s

def swap_last_first(name: str) -> str:
    # "Sánchez, Cristopher" -> "Cristopher Sánchez"
    if "," in name:
        last, first = [p.strip() for p in name.split(",", 1)]
        return f"{first} {last}".strip()
    return name

def all_name_variants(name: str) -> Iterable[str]:
    """Yield a set of normalized keys for robust matching."""
    if not isinstance(name, str):
        return []
    raw = name.strip()
    if not raw:
        return []

    variants = set()

    # raw & swapped (with accents)
    variants.add(norm_basic(raw))
    swapped = swap_last_first(raw)
    variants.add(norm_basic(swapped))

    # Also try collapsing multiple spaces and removing middle initials gracefully
    # e.g., "Jacob deGrom", "O'Neill, Tyler", etc. (norm_basic already strips punct/accents)
    return variants

def last_name_key(name: str) -> str:
    """Normalized last-name only key for fallback."""
    if not isinstance(name, str) or not name.strip():
        return ""
    # prefer the part before comma if "Last, First", else take the final token
    if "," in name:
        last = name.split(",", 1)[0].strip()
    else:
        parts = _WS_RE.split(name.strip())
        last = parts[-1] if parts else ""
    return norm_basic(last)

# ---- Data loading helpers ----

def read_csv_safe(path: Path, **kwargs) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[], **kwargs)
    except Exception:
        return None

def required(df: pd.DataFrame, cols: Iterable[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {where} missing columns: {missing}")

def to_int64(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

# ---- Build lookups ----

def build_name_to_id() -> Tuple[Dict[str, int], Dict[Tuple[str, str], set[int]]]:
    """
    Returns:
      name_key_to_id: normalized name key -> player_id
      team_last_to_ids: (team_id_str, last_name_key) -> {player_id,...}
    """
    name_key_to_id: Dict[str, int] = {}
    team_last_to_ids: Dict[Tuple[str, str], set[int]] = {}

    def add_mapping(name: str, pid_val, team_id: Optional[str] = None):
        try:
            pid = int(pd.to_numeric(pid_val, errors="coerce"))
        except Exception:
            return
        if pid <= 0:  # ignore placeholders here; only real, positive IDs
            return
        # By name variants
        for key in all_name_variants(name):
            # don't overwrite an existing mapping for the same key with a different id
            if key and key not in name_key_to_id:
                name_key_to_id[key] = pid
        # By team + last name (fallback)
        if isinstance(team_id, str) and team_id.strip():
            last_key = last_name_key(name)
            if last_key:
                team_last_to_ids.setdefault((team_id.strip(), last_key), set()).add(pid)

    # 1) Master file
    if MASTER_FILE.exists():
        df = read_csv_safe(MASTER_FILE) or pd.DataFrame()
        if not df.empty and {"name", "player_id"}.issubset(df.columns):
            # try to capture team_id if present
            for _, r in df.iterrows():
                add_mapping(str(r.get("name", "")).strip(), r.get("player_id"), str(r.get("team_id", "")).strip())

    # 2) Team pitchers files
    for p in glob.glob(TEAM_PITCHERS_GLOB):
        df = read_csv_safe(Path(p)) or pd.DataFrame()
        if not df.empty and {"name", "player_id"}.issubset(df.columns):
            for _, r in df.iterrows():
                add_mapping(str(r.get("name", "")).strip(), r.get("player_id"), str(r.get("team_id", "")).strip())

    # 3) Starting pitchers long (optional)
    if SP_LONG_FILE.exists():
        df = read_csv_safe(SP_LONG_FILE) or pd.DataFrame()
        # look for any id-ish column
        id_col = None
        for c in ("pitcher_id", "player_id", "mlb_id", "id"):
            if c in df.columns:
                id_col = c
                break
        if id_col and "name" in df.columns:
            for _, r in df.iterrows():
                add_mapping(str(r.get("name", "")).strip(), r.get(id_col), str(r.get("team_id", "")).strip())

    # 4) Overrides (authoritative)
    for k, v in OVERRIDES.items():
        add_mapping(k, v)

    return name_key_to_id, team_last_to_ids

# ---- Resolution logic ----

def is_placeholder(x) -> bool:
    """Treat negative/zero as placeholder/missing."""
    try:
        xi = int(pd.to_numeric(x, errors="coerce"))
        return xi <= 0
    except Exception:
        return True

def resolve_id_for_name(name: str, existing_id, team_id: Optional[str],
                        name_map: Dict[str, int],
                        team_last_map: Dict[Tuple[str, str], set[int]]) -> Optional[int]:
    # If existing is a valid positive int, keep it
    if pd.notna(existing_id) and not is_placeholder(existing_id):
        try:
            return int(existing_id)
        except Exception:
            pass

    # Ignore undecided
    if isinstance(name, str) and name.strip().lower() == "undecided":
        return None

    # Try normalized variants
    for key in all_name_variants(name):
        pid = name_map.get(key)
        if pid:
            return int(pid)

    # Team-aware LAST NAME fallback (only if exactly one candidate)
    if isinstance(team_id, str) and team_id.strip():
        lk = last_name_key(name)
        if lk:
            cands = team_last_map.get((team_id.strip(), lk), set())
            if len(cands) == 1:
                return int(next(iter(cands)))

    return None

# ---- Main ----

def load_games() -> pd.DataFrame:
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"{GAMES_FILE} not found")
    df = read_csv_safe(GAMES_FILE)
    if df is None:
        raise RuntimeError(f"Failed to read {GAMES_FILE}")
    req = {"game_id", "home_team", "away_team", "pitcher_home", "pitcher_away"}
    missing = req - set(df.columns)
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(missing)}")
    # Ensure id columns exist for passthrough
    for col in ("pitcher_home_id", "pitcher_away_id"):
        if col not in df.columns:
            df[col] = pd.NA
    # Team IDs are optional but help fallback
    for col in ("home_team_id", "away_team_id"):
        if col not in df.columns:
            df[col] = ""
    return df

def write_missing_report(rows: Iterable[dict]):
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    with open(MISSING_LOG, "w", newline="") as f:
        f.write("Pitcher ID placeholders assigned for unresolved starters:\n")
        for r in rows:
            f.write(
                f"game_id={r['game_id']}, "
                f"home={r['home_team']} (id={r.get('home_team_id','')}), "
                f"away={r['away_team']} (id={r.get('away_team_id','')}), "
                f"pitcher_home='{r['pitcher_home']}' -> {r['pitcher_home_id']}, "
                f"pitcher_away='{r['pitcher_away']}' -> {r['pitcher_away_id']}\n"
            )

def main():
    games = load_games()
    name_map, team_last_map = build_name_to_id()

    # Resolve home/away ids
    def _res_home(row):
        return resolve_id_for_name(
            row.get("pitcher_home", ""),
            row.get("pitcher_home_id", pd.NA),
            str(row.get("home_team_id", "")).strip(),
            name_map, team_last_map
        )

    def _res_away(row):
        return resolve_id_for_name(
            row.get("pitcher_away", ""),
            row.get("pitcher_away_id", pd.NA),
            str(row.get("away_team_id", "")).strip(),
            name_map, team_last_map
        )

    games["pitcher_home_id"] = games.apply(_res_home, axis=1)
    games["pitcher_away_id"] = games.apply(_res_away, axis=1)

    # Normalize dtype to pandas nullable Int64
    for col in ("pitcher_home_id", "pitcher_away_id"):
        games[col] = to_int64(games[col])

    # Log any still-missing
    missing_rows = []
    for _, r in games.iterrows():
        if pd.isna(r["pitcher_home_id"]) or pd.isna(r["pitcher_away_id"]):
            missing_rows.append({
                "game_id": r.get("game_id", ""),
                "home_team": r.get("home_team", ""),
                "away_team": r.get("away_team", ""),
                "home_team_id": r.get("home_team_id", ""),
                "away_team_id": r.get("away_team_id", ""),
                "pitcher_home": r.get("pitcher_home", ""),
                "pitcher_away": r.get("pitcher_away", ""),
                "pitcher_home_id": r.get("pitcher_home_id"),
                "pitcher_away_id": r.get("pitcher_away_id"),
            })
    if missing_rows:
        write_missing_report(missing_rows)

    # Persist
    GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_FILE, index=False)

if __name__ == "__main__":
    main()
