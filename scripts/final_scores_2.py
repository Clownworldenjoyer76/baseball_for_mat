#!/usr/bin/env python3
# scripts/final_scores_2.py
#
# Update game_props_history.csv with:
#   - proj_home_score
#   - proj_away_score
#   - projected_real_run_total
#   - favorite
#
# proj_team_runs = (batter_proj_runs + pitcher_allowed_runs) / 2
#
# Inputs:
#   data/bets/game_props_history.csv
#   data/bets/prep/batter_props_final.csv
#   data/bets/prep/pitcher_props_bets.csv
#
# Output (overwrite in place):
#   data/bets/game_props_history.csv
from __future__ import annotations
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import pandas as pd
import numpy as np
import re
import sys

GAME_OUT = Path("data/bets/game_props_history.csv")
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")

# ---- utils ----
def _std(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = d.columns.str.strip()
    d.columns = d.columns.str.lower()
    return d

def _read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        print(f"❌ Missing file: {path}")
        return None
    try:
        return pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"❌ Could not read {path}: {e}")
        return None

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
    if t_compact == "WHITESOX": return "WHITE SOX"
    if t_compact == "REDSOX": return "RED SOX"
    if t in SPECIALS: return SPECIALS[t]
    if t in {"WHITE SOX","RED SOX"}: return t
    parts = t.split(" ")
    if len(parts) >= 2:
        return " ".join(parts[-2:]) if parts[-2:] in (["WHITE","SOX"],["RED","SOX"]) else parts[-1]
    return t

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns: return c
    return None

# ---- load base table ----
base_raw = _read_csv(GAME_OUT)
if base_raw is None:
    sys.exit(1)
base = _std(base_raw)
for required in ["game_id","home_team","away_team"]:
    if required not in base.columns:
        print(f"❌ {GAME_OUT} missing column: {required}")
        sys.exit(1)
base["_game_id"] = base["game_id"].astype(str).str.strip()
base["_home_key"] = base["home_team"].apply(_canon_team_name)
base["_away_key"] = base["away_team"].apply(_canon_team_name)
for col in ["proj_home_score","proj_away_score","projected_real_run_total","favorite"]:
    if col not in base.columns: base[col] = pd.NA

# ---- batter file ----
bdf_raw = _read_csv(BATTER_FILE)
if bdf_raw is None: sys.exit(1)
bdf = _std(bdf_raw)
team_col_b = _pick_col(bdf, ["team_name","team","home_team","away_team"])
gid_col_b  = _pick_col(bdf, ["game_id","gamepk","game_pk","id"])
runs_col_b = _pick_col(bdf, ["projected_team_runs","batter_proj_runs","team_proj_runs","proj_runs","expected_runs"])
if team_col_b is None or gid_col_b is None or runs_col_b is None:
    print(f"❌ batter file columns not found. Have: {list(bdf.columns)}")
    sys.exit(1)
bdf["_team_key"] = bdf[team_col_b].apply(_canon_team_name)
bdf["_game_id"]  = bdf[gid_col_b].astype(str).str.strip()
bdf["_batter_proj_runs"] = _num(bdf[runs_col_b])
bdf_team = bdf.groupby(["_game_id","_team_key"], dropna=False)["_batter_proj_runs"].mean().reset_index()

# ---- pitcher file ----
pdf_raw = _read_csv(PITCHER_FILE)
if pdf_raw is None: sys.exit(1)
pdf = _std(pdf_raw)
team_col_p = _pick_col(pdf, ["team_name","team","home_team","away_team"])
gid_col_p  = _pick_col(pdf, ["game_id","gamepk","game_pk","id"])
runs_col_p = _pick_col(pdf, ["proj_runs_allowed","pitcher_allowed_runs","allowed_runs","x_runs_allowed"])
if team_col_p is None or gid_col_p is None or runs_col_p is None:
    print(f"❌ pitcher file columns not found. Have: {list(pdf.columns)}")
    sys.exit(1)
pdf["_team_key"] = pdf[team_col_p].apply(_canon_team_name)
pdf["_game_id"]  = pdf[gid_col_p].astype(str).str.strip()
pdf["_pitcher_allowed_runs"] = _num(pdf[runs_col_p])
pdf_team = pdf.groupby(["_game_id","_team_key"], dropna=False)["_pitcher_allowed_runs"].mean().reset_index()

# ---- combine per-team ----
team_proj = bdf_team.merge(pdf_team, on=["_game_id","_team_key"], how="inner")
team_proj["proj_team_runs"] = (team_proj["_batter_proj_runs"] + team_proj["_pitcher_allowed_runs"]) / 2.0

# ---- map to base home/away ----
home_merge = base[["_game_id","_home_key"]].merge(
    team_proj, left_on=["_game_id","_home_key"], right_on=["_game_id","_team_key"], how="left"
)
away_merge = base[["_game_id","_away_key"]].merge(
    team_proj, left_on=["_game_id","_away_key"], right_on=["_game_id","_team_key"], how="left"
)
base["proj_home_score"] = home_merge["proj_team_runs"]
base["proj_away_score"] = away_merge["proj_team_runs"]

base["projected_real_run_total"] = _num(base["proj_home_score"]) + _num(base["proj_away_score"])
base["favorite"] = np.where(
    _num(base["proj_home_score"]) > _num(base["proj_away_score"]), base["home_team"],
    np.where(_num(base["proj_away_score"]) > _num(base["proj_home_score"]), base["away_team"], pd.NA)
)

# preserve original col order, appending fields as needed
out_cols = list(base_raw.columns)
for c in ["proj_home_score","proj_away_score","projected_real_run_total","favorite"]:
    if c not in out_cols: out_cols.append(c)

base[out_cols].to_csv(GAME_OUT, index=False)
print(f"✅ Updated {len(base):,} rows → {GAME_OUT}")
