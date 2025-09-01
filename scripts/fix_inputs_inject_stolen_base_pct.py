# /mnt/data/baseball_for_mat-main/baseball_for_mat-main/scripts/fix_inputs_inject_stolen_base_pct.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pre-fix for batters_today.csv:
1) Inject r_stolen_base_pct from r_total_stolen_base and r_total_caught_stealing
2) Inject game_id by mapping team (clean name) to games.home_team (abbr) via team_directory
   Files:
     - data/cleaned/batters_today.csv
     - data/raw/todaysgames.csv (columns: game_id, home_team)
     - data/manual/team_directory.csv (columns: team_code, clean_team_name)
"""

import sys, os
import pandas as pd
import numpy as np

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BATTERS_CSV = os.path.join(REPO_ROOT, "data", "cleaned", "batters_today.csv")
GAMES_CSV   = os.path.join(REPO_ROOT, "data", "raw", "todaysgames.csv")
TEAMS_CSV   = os.path.join(REPO_ROOT, "data", "manual", "team_directory.csv")
SUMMARY_DIR = os.path.join(REPO_ROOT, "summaries", "pre_split")
SUMMARY_PATH = os.path.join(SUMMARY_DIR, "fix_inputs_summary.txt")

REQ_BATTERS = ["r_total_stolen_base", "r_total_caught_stealing", "team"]
REQ_GAMES   = ["game_id", "home_team"]
REQ_TEAMS   = ["team_code", "clean_team_name"]

TARGET_SB = "r_stolen_base_pct"
TARGET_GAME = "game_id"

def to_float_safe(x):
    try:
        if x is None: return np.nan
        s = str(x).strip()
        if s == "": return np.nan
        return float(s)
    except Exception:
        return np.nan

def inject_stolen_base(df):
    sbs = df["r_total_stolen_base"].map(to_float_safe)
    cs  = df["r_total_caught_stealing"].map(to_float_safe)
    attempts = sbs.add(cs)
    pct = pd.Series("", index=df.index, dtype=str)
    valid = attempts.fillna(0) > 0
    pct.loc[valid] = (sbs[valid] / attempts[valid]).astype(str)
    df[TARGET_SB] = pct
    return int((pct != "").sum()), int(len(df) - (pct != "").sum())

def inject_game_id(df):
    if not os.path.exists(GAMES_CSV):
        return 0, "MISSING: " + GAMES_CSV
    if not os.path.exists(TEAMS_CSV):
        return 0, "MISSING: " + TEAMS_CSV

    games = pd.read_csv(GAMES_CSV, dtype=str, keep_default_na=False, na_values=[])
    teams = pd.read_csv(TEAMS_CSV, dtype=str, keep_default_na=False, na_values=[])

    for need in REQ_GAMES:
        if need not in games.columns:
            return 0, f"MISSING COLUMN {need} in {GAMES_CSV}"
    for need in REQ_TEAMS:
        if need not in teams.columns:
            return 0, f"MISSING COLUMN {need} in {TEAMS_CSV}"

    code_to_clean = dict(zip(teams["team_code"], teams["clean_team_name"]))

    games = games.copy()
    games["home_clean"] = games["home_team"].map(code_to_clean).fillna("")
    gm = games.loc[games["home_clean"] != "", ["home_clean", "game_id"]].drop_duplicates()

    clean_to_gid = dict(zip(gm["home_clean"], gm["game_id"]))

    df[TARGET_GAME] = df["team"].map(clean_to_gid).fillna("")
    filled = int((df[TARGET_GAME] != "").sum())
    return filled, ""

def main():
    os.makedirs(SUMMARY_DIR, exist_ok=True)

    if not os.path.exists(BATTERS_CSV):
        msg = f"INSUFFICIENT INFORMATION: missing file {BATTERS_CSV}"
        with open(SUMMARY_PATH, "w") as f: f.write(msg + "\n")
        print(msg, file=sys.stderr); sys.exit(1)

    df = pd.read_csv(BATTERS_CSV, dtype=str, keep_default_na=False, na_values=[])

    miss = [c for c in REQ_BATTERS if c not in df.columns]
    if miss:
        msg = "INSUFFICIENT INFORMATION: missing cols in batters_today.csv: " + ", ".join(miss)
        with open(SUMMARY_PATH, "w") as f: f.write(msg + "\n")
        print(msg, file=sys.stderr); sys.exit(1)

    sb_filled, sb_blanks = inject_stolen_base(df)
    game_filled, game_err = inject_game_id(df)

    df.to_csv(BATTERS_CSV, index=False)

    with open(SUMMARY_PATH, "w") as f:
        f.write("batters_today.csv pre-fix complete\n")
        f.write(f"Rows: {len(df)}\n")
        f.write(f"{TARGET_SB} filled: {sb_filled}, blanks: {sb_blanks}\n")
        if game_err:
            f.write(f"{TARGET_GAME} injection error: {game_err}\n")
        else:
            f.write(f"{TARGET_GAME} filled: {game_filled}\n")

if __name__ == "__main__":
    main()
