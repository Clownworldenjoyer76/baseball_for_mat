#!/usr/bin/env python3
# scripts/final_scores_2.py
#
# Updates data/bets/game_props_history.csv with:
#   - proj_home_score
#   - proj_away_score
#   - projected_real_run_total
#   - favorite
#
# Logic:
#   1) Read game_props_history.csv (must have: game_id, home_team, away_team)
#   2) From batter file, group per (game_id, team) using a detected team-runs column
#   3) From pitcher file, group per (game_id, team) using a detected runs-allowed column
#   4) Merge those per-team values on (game_id, team)
#      proj_team_runs = mean(batter_team_runs, pitcher_allowed_runs) using whichever exist
#   5) Map to home/away by exact string match on (game_id, home_team) / (game_id, away_team)
#   6) Update the four columns; preserve all others; write back in place

from __future__ import annotations
from pathlib import Path
from typing import Optional, List
import pandas as pd
import numpy as np
import sys
import re

GAME_OUT     = Path("data/bets/game_props_history.csv")
BATTER_FILE  = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")

# ---------- helpers ----------
def _read_csv(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists():
        print(f"⚠️ Missing: {p}")
        return None
    try:
        return pd.read_csv(p, dtype=str)
    except Exception as e:
        print(f"⚠️ Could not read {p}: {e}")
        return None

def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(r"[,$%]", "", regex=True), errors="coerce")

def _first_present(df: pd.DataFrame, cands: List[str]) -> Optional[str]:
    for c in cands:
        if c in df.columns:
            return c
    return None

# ---------- load base ----------
base_raw = _read_csv(GAME_OUT)
if base_raw is None:
    sys.exit(1)

base = base_raw.copy()
required = ["game_id", "home_team", "away_team"]
missing = [c for c in required if c not in base.columns]
if missing:
    print(f"❌ {GAME_OUT} missing columns: {missing}")
    sys.exit(1)

# Ensure ID is string for consistent joins
base["game_id"] = base["game_id"].astype(str).str.strip()

# Prepare/ensure target columns exist
for col in ["proj_home_score", "proj_away_score", "projected_real_run_total", "favorite"]:
    if col not in base.columns:
        base[col] = pd.NA

# ---------- derive per-team projections from batter file ----------
bdf_raw = _read_csv(BATTER_FILE)
bdf_team = None
if bdf_raw is not None:
    bdf = bdf_raw.copy()
    if "game_id" in bdf.columns:
        bdf["game_id"] = bdf["game_id"].astype(str).str.strip()
    team_col = _first_present(bdf, ["team", "team_name", "home_team", "away_team"])
    gid_col  = "game_id" if "game_id" in bdf.columns else None
    # Candidate batter-side "team runs" columns (pick first present)
    batter_cols = [
        "projected_team_runs", "batter_proj_runs", "team_proj_runs",
        "proj_runs", "expected_runs"
    ]
    val_col = _first_present(bdf, batter_cols)
    if team_col and gid_col and val_col:
        tmp = bdf[[gid_col, team_col, val_col]].dropna(subset=[gid_col, team_col]).copy()
        tmp.rename(columns={gid_col: "game_id", team_col: "team", val_col: "b_runs"}, inplace=True)
        tmp["b_runs"] = _num(tmp["b_runs"])
        bdf_team = tmp.groupby(["game_id", "team"], dropna=False)["b_runs"].mean().reset_index()

# ---------- derive per-team projections from pitcher file ----------
pdf_raw = _read_csv(PITCHER_FILE)
pdf_team = None
if pdf_raw is not None:
    pdf = pdf_raw.copy()
    if "game_id" in pdf.columns:
        pdf["game_id"] = pdf["game_id"].astype(str).str.strip()
    team_col = _first_present(pdf, ["team", "team_name", "home_team", "away_team"])
    gid_col  = "game_id" if "game_id" in pdf.columns else None
    # Candidate pitcher-side "runs allowed" columns (pick first present)
    pitch_cols = [
        "proj_runs_allowed", "opp_total_runs_allowed", "pitcher_allowed_runs",
        "allowed_runs", "x_runs_allowed"
    ]
    val_col = _first_present(pdf, pitch_cols)
    if team_col and gid_col and val_col:
        tmp = pdf[[gid_col, team_col, val_col]].dropna(subset=[gid_col, team_col]).copy()
        tmp.rename(columns={gid_col: "game_id", team_col: "team", val_col: "p_runs"}, inplace=True)
        tmp["p_runs"] = _num(tmp["p_runs"])
        pdf_team = tmp.groupby(["game_id", "team"], dropna=False)["p_runs"].mean().reset_index()

# ---------- combine per-team values on (game_id, team) ----------
if bdf_team is None and pdf_team is None:
    # Nothing to update — write original back and exit cleanly
    base.to_csv(GAME_OUT, index=False)
    print(f"⚠️ No usable per-team columns found in inputs; left {GAME_OUT} unchanged.")
    sys.exit(0)

if bdf_team is not None and pdf_team is not None:
    team_proj = bdf_team.merge(pdf_team, on=["game_id", "team"], how="outer")
elif bdf_team is not None:
    team_proj = bdf_team.copy()
    team_proj["p_runs"] = pd.NA
else:
    team_proj = pdf_team.copy()
    team_proj["b_runs"] = pd.NA

# proj_team_runs = mean of available sides
team_proj["proj_team_runs"] = pd.concat(
    [ _num(team_proj.get("b_runs", pd.Series(dtype=float))),
      _num(team_proj.get("p_runs", pd.Series(dtype=float))) ],
    axis=1
).mean(axis=1, skipna=True)

# ---------- map to home/away using exact strings ----------
# Home
home_map = base[["game_id", "home_team"]].merge(
    team_proj, left_on=["game_id", "home_team"], right_on=["game_id", "team"], how="left"
)
base["proj_home_score"] = home_map["proj_team_runs"]

# Away
away_map = base[["game_id", "away_team"]].merge(
    team_proj, left_on=["game_id", "away_team"], right_on=["game_id", "team"], how="left"
)
base["proj_away_score"] = away_map["proj_team_runs"]

# Totals & favorite
hp = _num(base["proj_home_score"])
ap = _num(base["proj_away_score"])
base["projected_real_run_total"] = hp + ap
base["favorite"] = np.where(hp > ap, base["home_team"],
                     np.where(ap > hp, base["away_team"], pd.NA))

# ---------- save (preserve original column order; update/append targets) ----------
out_cols = list(base_raw.columns)
for c in ["proj_home_score","proj_away_score","projected_real_run_total","favorite"]:
    if c not in out_cols:
        out_cols.append(c)

base[out_cols].to_csv(GAME_OUT, index=False)
print(f"✅ Updated {len(base):,} rows → {GAME_OUT}")
