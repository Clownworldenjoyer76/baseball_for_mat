#!/usr/bin/env python3
# scripts/final_scores_2.py
#
# Update data/bets/game_props_history.csv with:
#   proj_home_score, proj_away_score, projected_real_run_total, favorite
#
# Formula per team when both sides exist:
#   proj_team_runs = mean(batter_team_runs, pitcher_allowed_runs)
#
# If only one side exists, use it. If neither exists, leave blank.

from __future__ import annotations
from pathlib import Path
from typing import Optional, Dict, Tuple, List
import pandas as pd
import numpy as np
import re
import sys

GAME_OUT     = Path("data/bets/game_props_history.csv")
BATTER_FILE  = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")

# ---------- helpers ----------
def _std(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = d.columns.str.strip().str.lower()
    return d

def _read_csv(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists():
        print(f"⚠️ missing: {p}")
        return None
    try:
        return pd.read_csv(p, dtype=str)
    except Exception as e:
        print(f"⚠️ cannot read {p}: {e}")
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

def _canon_team(s: str) -> str:
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
        return " ".join(parts[-2:]) if parts[-2:] in (["WHITE","SOX"], ["RED","SOX"]) else parts[-1]
    return t

def _first_present(d: pd.DataFrame, cands: List[str]) -> Optional[str]:
    for c in cands:
        if c in d.columns:
            return c
    return None

def _pick_team_col(df: pd.DataFrame) -> Optional[str]:
    return _first_present(df, ["team", "team_name", "home_team", "away_team"])

def _pick_game_id_col(df: pd.DataFrame) -> Optional[str]:
    return _first_present(df, ["game_id", "gamepk", "game_pk", "id"])

# ---------- load base ----------
base_raw = _read_csv(GAME_OUT)
if base_raw is None:
    sys.exit(1)
base = _std(base_raw)
need = ["game_id", "home_team", "away_team"]
missing = [c for c in need if c not in base.columns]
if missing:
    print(f"❌ {GAME_OUT} missing columns: {missing}")
    sys.exit(1)

base["_game_id"]  = base["game_id"].astype(str).str.strip()
base["_home_key"] = base["home_team"].apply(_canon_team)
base["_away_key"] = base["away_team"].apply(_canon_team)

for col in ["proj_home_score", "proj_away_score", "projected_real_run_total", "favorite"]:
    if col not in base.columns:
        base[col] = pd.NA

# ---------- load batter ----------
bdf_raw = _read_csv(BATTER_FILE)
bdf_team = None
if bdf_raw is not None:
    bdf = _std(bdf_raw)
    team_col_b = _pick_team_col(bdf)
    gid_col_b  = _pick_game_id_col(bdf)
    # candidate numeric columns for "batter team runs"
    batter_run_cols = [
        "projected_team_runs", "batter_proj_runs", "team_proj_runs",
        "proj_runs", "expected_runs"
    ]
    batter_val_col = _first_present(bdf, batter_run_cols)
    if team_col_b and batter_val_col:
        bdf["_team_key"] = bdf[team_col_b].apply(_canon_team)
        if gid_col_b:
            bdf["_game_id"] = bdf[gid_col_b].astype(str).str.strip()
            group_keys = ["_game_id", "_team_key"]
        else:
            bdf["_game_id"] = pd.NA
            group_keys = ["_team_key"]
        bdf["_batter_team_runs"] = _num(bdf[batter_val_col])
        bdf_team = (
            bdf.groupby(group_keys, dropna=False)["_batter_team_runs"]
              .mean()
              .reset_index()
        )

# ---------- load pitcher ----------
pdf_raw = _read_csv(PITCHER_FILE)
pdf_team = None
if pdf_raw is not None:
    pdf = _std(pdf_raw)
    team_col_p = _pick_team_col(pdf)
    gid_col_p  = _pick_game_id_col(pdf)
    pitcher_run_cols = [
        "proj_runs_allowed", "opp_total_runs_allowed", "pitcher_allowed_runs",
        "allowed_runs", "x_runs_allowed"
    ]
    pitcher_val_col = _first_present(pdf, pitcher_run_cols)
    if team_col_p and pitcher_val_col:
        pdf["_team_key"] = pdf[team_col_p].apply(_canon_team)
        if gid_col_p:
            pdf["_game_id"] = pdf[gid_col_p].astype(str).str.strip()
            group_keys = ["_game_id", "_team_key"]
        else:
            pdf["_game_id"] = pd.NA
            group_keys = ["_team_key"]
        pdf["_pitcher_allowed_runs"] = _num(pdf[pitcher_val_col])
        pdf_team = (
            pdf.groupby(group_keys, dropna=False)["_pitcher_allowed_runs"]
               .mean()
               .reset_index()
        )

# ---------- build team projections ----------
team_proj = None
if bdf_team is not None and pdf_team is not None:
    # join on both keys if present
    join_keys = ["_team_key"]
    if "_game_id" in bdf_team.columns and "_game_id" in pdf_team.columns:
        join_keys = ["_game_id", "_team_key"]
    team_proj = bdf_team.merge(pdf_team, on=join_keys, how="outer")
elif bdf_team is not None:
    team_proj = bdf_team.copy()
    team_proj["_pitcher_allowed_runs"] = pd.NA
elif pdf_team is not None:
    team_proj = pdf_team.copy()
    team_proj["_batter_team_runs"] = pd.NA

if team_proj is None or team_proj.empty:
    # nothing to update; write base back untouched
    base.to_csv(GAME_OUT, index=False)
    print(f"⚠️ No usable team-run columns found; left {GAME_OUT} unchanged.")
    sys.exit(0)

# compute per-team projection as mean of available sides
team_proj["proj_team_runs"] = pd.concat(
    [_num(team_proj.get("_batter_team_runs", pd.Series(dtype=float))),
     _num(team_proj.get("_pitcher_allowed_runs", pd.Series(dtype=float)))],
    axis=1
).mean(axis=1, skipna=True)

# ---------- map to home/away ----------
# Home side
h_keys = ["_team_key"]
if "_game_id" in team_proj.columns:
    h = base[["game_id","_game_id","_home_key"]].merge(
        team_proj, left_on=["_game_id","_home_key"], right_on=["_game_id","_team_key"], how="left"
    )
else:
    h = base[["game_id","_home_key"]].merge(
        team_proj, left_on=["_home_key"], right_on=["_team_key"], how="left"
    )
base["proj_home_score"] = h["proj_team_runs"]

# Away side
if "_game_id" in team_proj.columns:
    a = base[["game_id","_game_id","_away_key"]].merge(
        team_proj, left_on=["_game_id","_away_key"], right_on=["_game_id","_team_key"], how="left"
    )
else:
    a = base[["game_id","_away_key"]].merge(
        team_proj, left_on=["_away_key"], right_on=["_team_key"], how="left"
    )
base["proj_away_score"] = a["proj_team_runs"]

# Totals & favorite
hp = _num(base["proj_home_score"])
ap = _num(base["proj_away_score"])
base["projected_real_run_total"] = hp + ap
base["favorite"] = np.where(hp > ap, base["home_team"],
                     np.where(ap > hp, base["away_team"], pd.NA))

# ---------- save, preserving original order ----------
out_cols = list(base_raw.columns)
for c in ["proj_home_score","proj_away_score","projected_real_run_total","favorite"]:
    if c not in out_cols:
        out_cols.append(c)
base[out_cols].to_csv(GAME_OUT, index=False)
print(f"✅ Updated {len(base):,} rows → {GAME_OUT}")
