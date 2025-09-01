#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pre-fix inputs prior to 01 Split & Schedule.

Tasks:
1) Inject r_stolen_base_pct into data/cleaned/batters_today.csv
2) Inject game_id into data/cleaned/batters_today.csv via data/raw/todaysgames.csv + data/manual/team_directory.csv
   - Uses alias resolution and normalization
   - Matches BOTH home_team and away_team
3) Inject game_id into data/cleaned/pitchers_normalized_cleaned.csv using team_id
   - Uses data/manual/team_directory.csv to map team_id -> team_code
   - Maps team_code to game_id via BOTH home_team and away_team in data/raw/todaysgames.csv
   - If mapping is ambiguous (multiple game_ids for a team), leaves blank.

All operations are in-place and write a summary.
"""

import sys
import os
import pandas as pd
import numpy as np
import string

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

BATTERS_CSV  = os.path.join(REPO_ROOT, "data", "cleaned", "batters_today.csv")
PITCHERS_CSV = os.path.join(REPO_ROOT, "data", "cleaned", "pitchers_normalized_cleaned.csv")
GAMES_CSV    = os.path.join(REPO_ROOT, "data", "raw", "todaysgames.csv")
TEAMS_CSV    = os.path.join(REPO_ROOT, "data", "manual", "team_directory.csv")

SUMMARY_DIR  = os.path.join(REPO_ROOT, "summaries", "pre_split")
SUMMARY_PATH = os.path.join(SUMMARY_DIR, "fix_inputs_summary.txt")

REQ_BATTERS = ["r_total_stolen_base", "r_total_caught_stealing", "team"]
REQ_GAMES_B = ["game_id", "home_team", "away_team"]
REQ_TEAMS_B = ["team_code", "clean_team_name", "team_name", "canonical_team", "all_names", "all_codes"]

REQ_PITCHERS = ["team_id"]
REQ_GAMES_P  = ["game_id", "home_team", "away_team"]
REQ_TEAMS_P  = ["team_id", "team_code"]

TARGET_SB   = "r_stolen_base_pct"
TARGET_GAME = "game_id"

def to_float_safe(x):
    try:
        if x is None:
            return np.nan
        s = str(x).strip()
        if s == "":
            return np.nan
        return float(s)
    except Exception:
        return np.nan

_alnum = set(string.ascii_lowercase + string.digits)
def norm(s: str) -> str:
    """Normalize a string: lowercase, keep only a-z0-9."""
    if s is None:
        return ""
    s = str(s)
    out = []
    for ch in s.lower():
        if ch in _alnum:
            out.append(ch)
    return "".join(out)

def build_alias_maps(teams_df: pd.DataFrame):
    """
    Build:
      alias_to_code: normalized alias -> team_code
      id_to_code: team_id -> team_code
      code_to_clean: team_code -> clean_team_name
    Aliases come from team_name, clean_team_name, canonical_team, all_names, all_codes.
    """
    alias_to_code = {}
    id_to_code = {}
    code_to_clean = {}

    for _, row in teams_df.iterrows():
        code = str(row.get("team_code", "")).strip()
        tid  = str(row.get("team_id", "")).strip()
        clean = str(row.get("clean_team_name", "")).strip()

        if code:
            code_to_clean[code] = clean

        if tid and code:
            id_to_code[tid] = code

        aliases = set()

        # Primary names
        for key in ("team_name", "clean_team_name", "canonical_team"):
            v = str(row.get(key, "")).strip()
            if v:
                aliases.add(v)

        # Split alias lists
        for mkey, sep in (("all_names", "|"), ("all_codes", "|")):
            v = str(row.get(mkey, "")).strip()
            if v:
                for part in v.split(sep):
                    part = part.strip()
                    if part:
                        aliases.add(part)

        # Also include code itself as an alias
        if code:
            aliases.add(code)

        # Normalize and map to code
        for alias in aliases:
            k = norm(alias)
            if k and code:
                # Do not override existing mapping if present
                alias_to_code.setdefault(k, code)

    return alias_to_code, id_to_code, code_to_clean

def inject_stolen_base(df):
    sbs = df["r_total_stolen_base"].map(to_float_safe)
    cs  = df["r_total_caught_stealing"].map(to_float_safe)
    attempts = sbs.add(cs)
    pct = pd.Series("", index=df.index, dtype=str)
    valid = attempts.fillna(0) > 0
    pct.loc[valid] = (sbs[valid] / attempts[valid]).astype(str)
    df[TARGET_SB] = pct
    return int((pct != "").sum()), int(len(df) - (pct != "").sum())

def build_code_to_gids(games_df: pd.DataFrame):
    """Build mapping team_code -> set(game_id) from BOTH home and away."""
    code_to_gids = {}
    for _, row in games_df.iterrows():
        gid = str(row.get("game_id", "")).strip()
        h   = str(row.get("home_team", "")).strip()
        a   = str(row.get("away_team", "")).strip()
        if gid and h:
            code_to_gids.setdefault(h, set()).add(gid)
        if gid and a:
            code_to_gids.setdefault(a, set()).add(gid)
    return code_to_gids

def inject_batters_game_id(df):
    if not os.path.exists(GAMES_CSV):
        return 0, "MISSING: " + GAMES_CSV
    if not os.path.exists(TEAMS_CSV):
        return 0, "MISSING: " + TEAMS_CSV

    games = pd.read_csv(GAMES_CSV, dtype=str, keep_default_na=False, na_values=[])
    teams = pd.read_csv(TEAMS_CSV, dtype=str, keep_default_na=False, na_values=[])

    # Validate required columns
    for need in REQ_GAMES_B:
        if need not in games.columns:
            return 0, f"MISSING COLUMN {need} in {GAMES_CSV}"
    for need in REQ_TEAMS_B:
        if need not in teams.columns:
            return 0, f"MISSING COLUMN {need} in {TEAMS_CSV}"

    alias_to_code, _, _ = build_alias_maps(teams)
    code_to_gids = build_code_to_gids(games)

    # Resolve each batter's team to a team_code via aliases, then to game_id
    out = []
    ambiguous = 0
    for t in df["team"].astype(str):
        code = alias_to_code.get(norm(t), "")
        gids = code_to_gids.get(code, set())
        if len(gids) == 1:
            out.append(next(iter(gids)))
        else:
            out.append("")
            if len(gids) > 1:
                ambiguous += 1

    df[TARGET_GAME] = pd.Series(out, index=df.index, dtype=str)
    filled = int((df[TARGET_GAME] != "").sum())
    return filled, (f"AMBIGUOUS_TEAMS:{ambiguous}" if ambiguous else "")

def inject_pitchers_game_id(df):
    if not os.path.exists(GAMES_CSV):
        return 0, "MISSING: " + GAMES_CSV
    if not os.path.exists(TEAMS_CSV):
        return 0, "MISSING: " + TEAMS_CSV
    if "team_id" not in df.columns:
        return 0, "MISSING COLUMN team_id in pitchers file"

    games = pd.read_csv(GAMES_CSV, dtype=str, keep_default_na=False, na_values=[])
    teams = pd.read_csv(TEAMS_CSV, dtype=str, keep_default_na=False, na_values=[])

    for need in REQ_GAMES_P:
        if need not in games.columns:
            return 0, f"MISSING COLUMN {need} in {GAMES_CSV}"
    for need in REQ_TEAMS_P:
        if need not in teams.columns:
            return 0, f"MISSING COLUMN {need} in {TEAMS_CSV}"

    _, id_to_code, _ = build_alias_maps(teams)
    code_to_gids = build_code_to_gids(games)

    out = []
    ambiguous = 0
    for tid in df["team_id"].astype(str):
        code = id_to_code.get(tid, "")
        gids = code_to_gids.get(code, set())
        if len(gids) == 1:
            out.append(next(iter(gids)))
        else:
            out.append("")
            if len(gids) > 1:
                ambiguous += 1

    df[TARGET_GAME] = pd.Series(out, index=df.index, dtype=str)
    filled = int((df[TARGET_GAME] != "").sum())
    return filled, (f"AMBIGUOUS_TEAMS:{ambiguous}" if ambiguous else "")

def main():
    os.makedirs(SUMMARY_DIR, exist_ok=True)

    # BATTERS
    if os.path.exists(BATTERS_CSV):
        bdf = pd.read_csv(BATTERS_CSV, dtype=str, keep_default_na=False, na_values=[])
        miss = [c for c in REQ_BATTERS if c not in bdf.columns]
        if miss:
            b_summary = "INSUFFICIENT INFORMATION: batters missing " + ", ".join(miss) + "\n"
        else:
            sb_filled, sb_blanks = inject_stolen_base(bdf)
            b_game_filled, b_game_err = inject_batters_game_id(bdf)
            bdf.to_csv(BATTERS_CSV, index=False)
            b_summary = (
                f"BATTERS rows: {len(bdf)}\n"
                f"- {TARGET_SB} filled: {sb_filled}, blanks: {sb_blanks}\n"
                f"- {TARGET_GAME} filled: {b_game_filled}" + (f" | error: {b_game_err}\n" if b_game_err else "\n")
            )
    else:
        b_summary = f"INSUFFICIENT INFORMATION: missing file {BATTERS_CSV}\n"

    # PITCHERS
    if os.path.exists(PITCHERS_CSV):
        pdf = pd.read_csv(PITCHERS_CSV, dtype=str, keep_default_na=False, na_values=[])
        if "team_id" not in pdf.columns:
            p_summary = "INSUFFICIENT INFORMATION: pitchers missing team_id\n"
        else:
            p_game_filled, p_game_err = inject_pitchers_game_id(pdf)
            pdf.to_csv(PITCHERS_CSV, index=False)
            p_summary = (
                f"PITCHERS rows: {len(pdf)}\n"
                f"- {TARGET_GAME} filled: {p_game_filled}" + (f" | error: {p_game_err}\n" if p_game_err else "\n")
            )
    else:
        p_summary = f"INSUFFICIENT INFORMATION: missing file {PITCHERS_CSV}\n"

    with open(SUMMARY_PATH, "w") as f:
        f.write("Pre-fix summary\n")
        f.write(b_summary)
        f.write(p_summary)

if __name__ == "__main__":
    main()
