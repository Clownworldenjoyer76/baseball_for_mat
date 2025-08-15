#!/usr/bin/env python3
# scripts/final_scores_2.py
#
# Update game_props_history.csv with:
#   - proj_home_score
#   - proj_away_score
#   - projected_real_run_total
#   - favorite
#
# Data sources (column mapping hard-coded to your files):
#   Batter:  data/bets/prep/batter_props_final.csv
#       team_name, game_id, projected_team_runs
#   Pitcher: data/bets/prep/pitcher_props_bets.csv
#       team_name, game_id, proj_runs_allowed
#
# Formula per team:
#   proj_team_runs = (projected_team_runs + proj_runs_allowed) / 2
#
# Output overwritten in place:
#   data/bets/game_props_history.csv

from __future__ import annotations
from pathlib import Path
from typing import Dict
import pandas as pd
import numpy as np
import re
import sys

GAME_OUT = Path("data/bets/game_props_history.csv")
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")

def _std(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = d.columns.str.strip()
    return d

def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(r"[,$%]", "", regex=True), errors="coerce")

SPECIALS: Dict[str, str] = {
    "ARIZONA DIAMONDBACKS": "DIAMONDBACKS",
    "BOSTON RED SOX": "RED SOX",
    "CHICAGO WHITE SOX": "WHITE SOX",
    "ST LOUIS CARDINALS": "CARDINALS",
    "ST. LOUIS CARDINALS": "CARDINALS",
    "TORONTO BLUE JAYS": "BLUE JAYS",
    "D-BACKS": "DIAMONDBACKS",
    "DBACKS": "DIAMONDBACKS",
}

def _canon_team_name(s: str) -> str:
    if s is None:
        return ""
    t = re.sub(r"\s+", " ", str(s).strip().upper())
    t_compact = re.sub(r"[^A-Z]", "", t)
    if t_compact == "WHITESOX":
        return "WHITE SOX"
    if t_compact == "REDSOX":
        return "RED SOX"
    if t in SPECIALS:
        return SPECIALS[t]
    if t in {"WHITE SOX", "RED SOX"}:
        return t
    parts = t.split(" ")
    if len(parts) >= 2:
        return " ".join(parts[-2:]) if parts[-2:] in (["WHITE","SOX"],["RED","SOX"]) else parts[-1]
    return t

def main() -> None:
    # Load current game_props_history
    try:
        base = pd.read_csv(GAME_OUT, dtype=str)
    except Exception as e:
        print(f"❌ Cannot read {GAME_OUT}: {e}")
        sys.exit(1)
    base = _std(base)

    for need in ["game_id","home_team","away_team"]:
        if need not in base.columns:
            print(f"❌ {GAME_OUT} missing column: {need}")
            sys.exit(1)

    base["_game_id"] = base["game_id"].astype(str).str.strip()
    base["_home_key"] = base["home_team"].apply(_canon_team_name)
    base["_away_key"] = base["away_team"].apply(_canon_team_name)

    # Ensure target columns exist
    for col in ["proj_home_score","proj_away_score","projected_real_run_total","favorite"]:
        if col not in base.columns:
            base[col] = pd.NA

    # Batter file (hard-coded columns)
    try:
        bdf = pd.read_csv(BATTER_FILE, dtype=str)
    except Exception as e:
        print(f"❌ Cannot read {BATTER_FILE}: {e}")
        sys.exit(1)
    bdf = _std(bdf)
    for c in ["team_name","game_id","projected_team_runs"]:
        if c not in bdf.columns:
            print(f"❌ {BATTER_FILE} missing column: {c}")
            sys.exit(1)
    bdf["_team_key"] = bdf["team_name"].apply(_canon_team_name)
    bdf["_game_id"] = bdf["game_id"].astype(str).str.strip()
    bdf["_b_runs"] = _num(bdf["projected_team_runs"])
    b_team = bdf.groupby(["_game_id","_team_key"], dropna=False)["_b_runs"].mean().reset_index()

    # Pitcher file (hard-coded columns)
    try:
        pdf = pd.read_csv(PITCHER_FILE, dtype=str)
    except Exception as e:
        print(f"❌ Cannot read {PITCHER_FILE}: {e}")
        sys.exit(1)
    pdf = _std(pdf)
    for c in ["team_name","game_id","proj_runs_allowed"]:
        if c not in pdf.columns:
            print(f"❌ {PITCHER_FILE} missing column: {c}")
            sys.exit(1)
    pdf["_team_key"] = pdf["team_name"].apply(_canon_team_name)
    pdf["_game_id"] = pdf["game_id"].astype(str).str.strip()
    pdf["_p_runs"] = _num(pdf["proj_runs_allowed"])
    p_team = pdf.groupby(["_game_id","_team_key"], dropna=False)["_p_runs"].mean().reset_index()

    # Combine per-team
    team_proj = b_team.merge(p_team, on=["_game_id","_team_key"], how="inner")
    team_proj["proj_team_runs"] = (team_proj["_b_runs"] + team_proj["_p_runs"]) / 2.0

    # Map to home/away
    home = base[["_game_id","_home_key"]].merge(team_proj, left_on=["_game_id","_home_key"], right_on=["_game_id","_team_key"], how="left")
    away = base[["_game_id","_away_key"]].merge(team_proj, left_on=["_game_id","_away_key"], right_on=["_game_id","_team_key"], how="left")

    base["proj_home_score"] = home["proj_team_runs"]
    base["proj_away_score"] = away["proj_team_runs"]

    # Totals and favorite
    h = _num(base["proj_home_score"])
    a = _num(base["proj_away_score"])
    base["projected_real_run_total"] = h + a
    base["favorite"] = np.where(h > a, base["home_team"], np.where(a > h, base["away_team"], pd.NA))

    # Save back preserving original column order (append our fields if new)
    out_cols = list(pd.read_csv(GAME_OUT, nrows=0).columns)
    for c in ["proj_home_score","proj_away_score","projected_real_run_total","favorite"]:
        if c not in out_cols:
            out_cols.append(c)
    base[out_cols].to_csv(GAME_OUT, index=False)
    print(f"✅ Updated {len(base):,} rows → {GAME_OUT}")

if __name__ == "__main__":
    main()
