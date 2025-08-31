#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import pandas as pd
from pathlib import Path

INPUT = Path("data/raw/todaysgames.csv")
OUTPUT = Path("data/raw/todaysgames_normalized.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")

# ---------- helpers ----------

def _norm_key(s: str) -> str:
    """Uppercase and strip non-alphanumerics so alias matching is forgiving."""
    if not isinstance(s, str):
        return ""
    s = s.strip().upper()
    return re.sub(r"[^A-Z0-9]", "", s)

def _explode(col_val) -> list:
    """
    Split a cell into multiple aliases.
    Supports comma, pipe, semicolon, slash as delimiters.
    """
    if pd.isna(col_val):
        return []
    text = str(col_val)
    # Normalize delimiters to comma, then split
    text = re.sub(r"[|;/]", ",", text)
    parts = [p.strip() for p in text.split(",") if p.strip()]
    return parts

# ---------- core ----------

def load_team_directory() -> pd.DataFrame:
    """
    Load data/manual/team_directory.csv and standardize expected columns:
      team_id, team_code, canonical_team, team_name, clean_team_name, all_codes, all_names
    """
    df = pd.read_csv(TEAM_DIR)
    # lower headers and unify names if user varied capitalization
    cols = {c: c.strip().lower() for c in df.columns}
    df.columns = [cols[c] for c in df.columns]

    # Ensure required columns exist
    required = {
        "team_id", "team_code", "canonical_team",
        "team_name", "clean_team_name", "all_codes", "all_names"
    }
    missing = [c for c in required if c not in df.columns]
    if missing:
        sys.exit(f"ERROR normalize_todays_games.py: team_directory missing columns: {missing}")

    # Coerce types
    if df["team_id"].dtype.kind not in "iu":
        # Make sure team_id is numeric if the CSV wrote it as text
        df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")

    # Team code as string upper
    df["team_code"] = df["team_code"].astype(str).str.strip().str.upper()

    return df


def build_alias_maps(team_df: pd.DataFrame):
    """
    Build:
      alias_to_code: maps many alias forms -> team_code
      code_to_id:    maps team_code -> team_id
    """
    alias_to_code = {}
    code_to_id = {}

    for _, r in team_df.iterrows():
        code = str(r["team_code"]).strip().upper()
        tid = r["team_id"]

        code_to_id[code] = int(tid) if pd.notna(tid) else None

        # Primary names/aliases
        primals = set()

        for col in ("team_name", "canonical_team", "clean_team_name"):
            v = r.get(col)
            if pd.notna(v):
                primals.add(str(v))

        # All codes and names
        for alias in _explode(r.get("all_codes")) + _explode(r.get("all_names")):
            primals.add(alias)

        # Also include the code itself
        primals.add(code)

        # Register both raw and normalized keys
        for alias in primals:
            if not alias:
                continue
            alias_raw = alias.strip().upper()
            alias_norm = _norm_key(alias_raw)

            # raw key
            if alias_raw and alias_raw not in alias_to_code:
                alias_to_code[alias_raw] = code
            # normalized key
            if alias_norm and alias_norm not in alias_to_code:
                alias_to_code[alias_norm] = code

    return alias_to_code, code_to_id


def normalize():
    # Load inputs
    if not INPUT.exists():
        sys.exit(f"ERROR normalize_todays_games.py: missing input {INPUT}")

    games = pd.read_csv(INPUT)
    teams = load_team_directory()
    alias_to_code, code_to_id = build_alias_maps(teams)

    # Validate expected game columns
    needed_cols = {"home_team", "away_team"}
    missing = [c for c in needed_cols if c not in games.columns]
    if missing:
        sys.exit(f"ERROR normalize_todays_games.py: todaysgames.csv missing columns: {missing}")

    # Map function using both raw and normalized keys
    def map_team(value: str) -> str:
        if pd.isna(value):
            return value
        raw = str(value).strip().upper()
        key_norm = _norm_key(raw)
        return alias_to_code.get(raw, alias_to_code.get(key_norm, raw))

    # Normalize to canonical team_code
    games = games.copy()
    games["home_team"] = games["home_team"].apply(map_team)
    games["away_team"] = games["away_team"].apply(map_team)

    # Attach IDs from team_code
    games["home_team_id"] = games["home_team"].map(code_to_id)
    games["away_team_id"] = games["away_team"].map(code_to_id)

    # Output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(OUTPUT, index=False)
    print(f"✅ normalize_todays_games wrote {len(games)} rows -> {OUTPUT}")


if __name__ == "__main__":
    normalize()
