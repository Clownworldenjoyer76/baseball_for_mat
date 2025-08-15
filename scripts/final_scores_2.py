#!/usr/bin/env python3
# scripts/final_scores_2.py
#
# Update game_props_history.csv with:
#   - proj_home_score
#   - proj_away_score
#   - projected_real_run_total
#   - favorite
#
# Formula per team:
#   proj_team_runs = (batter_proj_runs + pitcher_allowed_runs) / 2
#
# Inputs (read-only):
#   data/bets/game_props_history.csv
#   data/bets/prep/batter_props_final.csv
#   data/bets/prep/pitcher_props_bets.csv
#
# Output (overwrite in place):
#   data/bets/game_props_history.csv

from __future__ import annotations
from pathlib import Path
from typing import Optional, Tuple, Dict
import pandas as pd
import numpy as np
import re
import sys

GAME_OUT = Path("data/bets/game_props_history.csv")
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")

# ----- helpers -----
def _std(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = d.columns.str.strip().str.lower()
    return d

def _read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        print(f"❌ Missing: {path}")
        return None
    try:
        return pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"❌ Could not read {path}: {e}")
        return None

def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(r"[,$%]", "", regex=True), errors="coerce")

# Canonical team keys to align full names/nicknames/compact forms
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
        # keep Sox as two-word nickname
        return " ".join(parts[-2:]) if parts[-2:] in (["WHITE", "SOX"], ["RED", "SOX"]) else parts[-1]
    return t

def _first_present(d: pd.DataFrame, cands) -> Optional[str]:
    for c in cands:
        if c in d.columns:
            return c
    return None

def _pick_team_col(df: pd.DataFrame) -> Optional[str]:
    return _first_present(df, ["team", "team_name", "home_team", "away_team"])

def _pick_game_id_col(df: pd.DataFrame) -> Optional[str]:
    return _first_present(df, ["game_id", "gamepk", "game_pk", "id"])

# ----- load base output -----
base_raw = _read_csv(GAME_OUT)
if base_raw is None:
    sys.exit(1)
base = _std(base_raw)
needed_base = ["game_id", "home_team", "away_team"]
miss = [c for c in needed_base if c not in base.columns]
if miss:
    print(f"❌ {GAME_OUT} missing columns: {miss}")
    sys.exit(1)

# Prepare base keys
base["_home_key"] = base["home_team"].apply(_canon_team_name)
base["_away_key"] = base["away_team"].apply(_canon_team_name)

# Ensure columns to be updated exist
for col in ["proj_home_score", "proj_away_score", "projected_real_run_total", "favorite"]:
    if col not in base.columns:
        base[col] = pd.NA

# ----- load batter projections -----
bdf_raw = _read_csv(BATTER_FILE)
if bdf_raw is None:
    sys.exit(1)
bdf = _std(bdf_raw)

team_col_b = _pick_team_col(bdf)
gid_col_b = _pick_game_id_col(bdf)
if team_col_b is None or gid_col_b is None:
    print("❌ batter file must contain team and game_id columns.")
    sys.exit(1)

if "batter_proj_runs" not in bdf.columns:
    print("❌ batter file missing required column: batter_proj_runs")
    sys.exit(1)

bdf["_team_key"] = bdf[team_col_b].apply(_canon_team_name)
bdf["_game_id"] = bdf[gid_col_b].astype(str).str.strip()
bdf["_batter_proj_runs"] = _num(bdf["batter_proj_runs"])

bdf_team = (
    bdf.groupby(["_game_id", "_team_key"], dropna=False)["_batter_proj_runs"]
    .mean()
    .reset_index()
)

# ----- load pitcher projections -----
pdf_raw = _read_csv(PITCHER_FILE)
if pdf_raw is None:
    sys.exit(1)
pdf = _std(pdf_raw)

team_col_p = _pick_team_col(pdf)
gid_col_p = _pick_game_id_col(pdf)
if team_col_p is None or gid_col_p is None:
    print("❌ pitcher file must contain team and game_id columns.")
    sys.exit(1)
if "pitcher_allowed_runs" not in pdf.columns:
    print("❌ pitcher file missing required column: pitcher_allowed_runs")
    sys.exit(1)

pdf["_team_key"] = pdf[team_col_p].apply(_canon_team_name)
pdf["_game_id"] = pdf[gid_col_p].astype(str).str.strip()
pdf["_pitcher_allowed_runs"] = _num(pdf["pitcher_allowed_runs"])

pdf_team = (
    pdf.groupby(["_game_id", "_team_key"], dropna=False)["_pitcher_allowed_runs"]
    .mean()
    .reset_index()
)

# ----- combine per-team projections -----
team_proj = bdf_team.merge(
    pdf_team, on=["_game_id", "_team_key"], how="inner"
)
team_proj["proj_team_runs"] = (
    team_proj["_batter_proj_runs"] + team_proj["_pitcher_allowed_runs"]
) / 2.0

# ----- map to home/away and update base -----
base["_game_id"] = base["game_id"].astype(str).str.strip()

# Home
home_merge = base[["game_id", "_game_id", "_home_key"]].merge(
    team_proj, left_on=["_game_id", "_home_key"], right_on=["_game_id", "_team_key"], how="left"
)
base["proj_home_score"] = home_merge["proj_team_runs"]

# Away
away_merge = base[["game_id", "_game_id", "_away_key"]].merge(
    team_proj, left_on=["_game_id", "_away_key"], right_on=["_game_id", "_team_key"], how="left"
)
base["proj_away_score"] = away_merge["proj_team_runs"]

# Totals + favorite
base["projected_real_run_total"] = (
    _num(base["proj_home_score"]) + _num(base["proj_away_score"])
)

base["favorite"] = np.where(
    _num(base["proj_home_score"]) > _num(base["proj_away_score"]),
    base["home_team"],
    np.where(
        _num(base["proj_away_score"]) > _num(base["proj_home_score"]),
        base["away_team"],
        pd.NA,
    ),
)

# Save
out_cols = list(base_raw.columns)  # keep original order first
# Ensure our 4 columns are present and placed at the end if not already in order
for c in ["proj_home_score", "proj_away_score", "projected_real_run_total", "favorite"]:
    if c not in out_cols:
        out_cols.append(c)

base[out_cols].to_csv(GAME_OUT, index=False)
print(f"✅ Updated {len(base):,} rows → {GAME_OUT}")
