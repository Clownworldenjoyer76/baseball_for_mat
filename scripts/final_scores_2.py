#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Inputs (read-only):
  - data/raw/todaysgames_normalized.csv
  - data/bets/prep/batter_props_final.csv
  - data/bets/prep/pitcher_props_bets.csv

Output (overwrite):
  - data/bets/game_props_history.csv

Columns left BLANK:
  favorite_correct, actual_real_run_total, run_total_diff, home_score, away_score
"""

from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
import re

# ---------------- Paths ----------------
GAMES    = Path("data/raw/todaysgames_normalized.csv")
BATTERS  = Path("data/bets/prep/batter_props_final.csv")
PITCHERS = Path("data/bets/prep/pitcher_props_bets.csv")
OUTFILE  = Path("data/bets/game_props_history.csv")

# ---------------- Model constants ----------------
BASELINE = 4.5   # league-average runs per team
ALPHA    = 0.8   # lineup z impact
BETA     = 0.8   # opposing SP z impact

# ---------------- Team normalization (deterministic; no fuzzy) ----------------
CANON = {
    "diamondbacks":"Diamondbacks","braves":"Braves","orioles":"Orioles","red sox":"Red Sox",
    "cubs":"Cubs","white sox":"White Sox","reds":"Reds","guardians":"Guardians","rockies":"Rockies",
    "tigers":"Tigers","astros":"Astros","royals":"Royals","angels":"Angels","dodgers":"Dodgers",
    "marlins":"Marlins","brewers":"Brewers","twins":"Twins","mets":"Mets","yankees":"Yankees",
    "athletics":"Athletics","phillies":"Phillies","pirates":"Pirates","padres":"Padres",
    "giants":"Giants","mariners":"Mariners","cardinals":"Cardinals","rays":"Rays","rangers":"Rangers",
    "blue jays":"Blue Jays","nationals":"Nationals",
}
ALIASES = {
    # common variants/misspellings
    "d-backs":"diamondbacks","dbacks":"diamondbacks","diamondiamondbacks":"diamondbacks",
    "whitesox":"white sox","chi white sox":"white sox","chi sox":"white sox",
    "ny yankees":"yankees","st. louis cardinals":"cardinals","la angels":"angels",
    "los angeles angels":"angels","oakland athletics":"athletics","tb rays":"rays",
    "tampa bay devil rays":"rays","toronto blue jays":"blue jays","washington nationals":"nationals",
}

def _collapse_repeat_word(s: str) -> str:
    # Fix sequences like "Diamondiamondbacks" -> "Diamondbacks"
    return re.sub(r'(?:([a-z]{3,}))\1+', r'\1', s, flags=re.I)

def norm_team(x: str) -> str:
    if not isinstance(x, str):
        return ""
    s = _collapse_repeat_word(x.strip())
    s = re.sub(r"\s+", " ", s)
    low = s.lower()
    low = ALIASES.get(low, low)
    return CANON.get(low, s.title())

# ---------------- Utils ----------------
def prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    for c in ("team","opp_team","home_team","away_team","game_id","date","matchup"):
        if c in df.columns:
            df[c] = df[c].astype("string").fillna("").str.strip()
    if "date" in df.columns:
        parsed = pd.to_datetime(df["date"], errors="coerce", utc=False)
        df["date"] = parsed.dt.date.astype("string").fillna("")
    return df

def ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

# ---------------- Load ----------------
if not GAMES.exists():   raise SystemExit(f"Missing input: {GAMES}")
if not BATTERS.exists(): raise SystemExit(f"Missing input: {BATTERS}")
if not PITCHERS.exists():raise SystemExit(f"Missing input: {PITCHERS}")

games   = prep(pd.read_csv(GAMES))
batters = prep(pd.read_csv(BATTERS))
pitch   = prep(pd.read_csv(PITCHERS))

# ---------------- Prepare games: authoritative date/home/away/game_id ----------------
if "home_team" not in games.columns or "away_team" not in games.columns:
    # Try to parse from 'matchup' like "Away @ Home"
    if "matchup" in games.columns:
        away, home = [], []
        for s in games["matchup"].astype("string").fillna(""):
            parts = re.split(r"@|\bat\b", s)
            if len(parts) >= 2:
                away.append(parts[0].strip()); home.append(parts[1].strip())
            else:
                away.append(""); home.append("")
        games["away_team"] = pd.Series(away, dtype="string")
        games["home_team"] = pd.Series(home, dtype="string")
    else:
        raise SystemExit("todaysgames_normalized.csv must contain home_team and away_team or a parsable 'matchup'.")

games["home_team"] = games["home_team"].map(norm_team)
games["away_team"] = games["away_team"].map(norm_team)

# Keep only necessary columns; do NOT create game_id if it's missing
if "date" not in games.columns:
    games["date"] = ""  # keep blank if absent
core_games_cols = [c for c in ["date","game_id","home_team","away_team"] if c in games.columns]
games = games[core_games_cols].drop_duplicates().reset_index(drop=True)

# ---------------- Build offense strength from batter props ----------------
# Choose strength metric
if "mega_z" in batters.columns:
    bat_strength_col = "mega_z"
elif "batter_z" in batters.columns:
    bat_strength_col = "batter_z"
else:
    bat_strength_col = "_zero"
    batters[bat_strength_col] = 0.0

# Weighting
if "over_probability" in batters.columns:
    w = pd.to_numeric(batters["over_probability"], errors="coerce").clip(0.0, 1.0).fillna(0.75)
else:
    w = pd.Series(1.0, index=batters.index)

# Normalize team keys
for c in ("team","opp_team"):
    if c in batters.columns:
        batters[c] = batters[c].map(norm_team)

group_keys = [k for k in ["date","team","opp_team"] if k in batters.columns]
if not group_keys:
    group_keys = ["team","opp_team"]

off_rows = []
for keys, df in batters.groupby(group_keys, dropna=False):
    if not isinstance(keys, tuple): keys = (keys,)
    kdict = dict(zip(group_keys, [str(k) for k in keys]))
    off = np.nan
    try:
        off = float(np.average(pd.to_numeric(df[bat_strength_col], errors="coerce"), 
                               weights=pd.to_numeric(w.loc[df.index], errors="coerce")))
    except Exception:
        off = float(pd.to_numeric(df[bat_strength_col], errors="coerce").mean())
    if not np.isfinite(off): off = 0.0
    off_rows.append({**kdict, "offense_strength_z": off})

off_df = pd.DataFrame(off_rows)
for c in ("date","team","opp_team"):
    if c in off_df.columns:
        off_df[c] = off_df[c].astype("string").fillna("")

# ---------------- Build SP strength from pitcher props (per date, team) ----------------
# Choose pitcher strength metric
if "mega_z" in pitch.columns:
    sp_strength_col = "mega_z"
elif "z_score" in pitch.columns:
    sp_strength_col = "z_score"
else:
    sp_strength_col = "_zero"
    pitch[sp_strength_col] = 0.0

# Weighting
if "over_probability" in pitch.columns:
    pw = pd.to_numeric(pitch["over_probability"], errors="coerce").clip(0.0, 1.0).fillna(0.75)
else:
    pw = pd.Series(1.0, index=pitch.index)

# Normalize team field
if "team" not in pitch.columns:
    pitch["team"] = ""
pitch["team"] = pitch["team"].map(norm_team)

sp_keys = [k for k in ["date","team"] if k in pitch.columns]
if not sp_keys:
    sp_keys = ["team"]

sp_rows = []
for keys, df in pitch.groupby(sp_keys, dropna=False):
    if not isinstance(keys, tuple): keys = (keys,)
    kdict = dict(zip(sp_keys, [str(k) for k in keys]))
    # pick the row with highest weight as the presumed listed starter
    idx = pw.loc[df.index].astype(float).fillna(0.0).idxmax()
    val = float(pd.to_numeric(df.loc[idx, sp_strength_col], errors="coerce")) if idx in df.index else 0.0
    if not np.isfinite(val): val = 0.0
    sp_rows.append({**kdict, "sp_strength_z": val})

sp_df = pd.DataFrame(sp_rows)
for c in sp_keys:
    sp_df[c] = sp_df[c].astype("string").fillna("")

# Helpers to fetch strengths with date-aware fallback
def get_offense_z(team: str, opp: str, date_val: str) -> float:
    if {"date","team","opp_team"}.issubset(off_df.columns):
        m = (off_df["team"] == team) & (off_df["opp_team"] == opp) & (off_df["date"] == date_val)
        if m.any(): 
            v = float(off_df.loc[m, "offense_strength_z"].iloc[0])
            return v if np.isfinite(v) else 0.0
    # fallback without date
    m2 = (off_df.get("team","") == team) & (off_df.get("opp_team","") == opp)
    if m2.any():
        v = float(off_df.loc[m2, "offense_strength_z"].iloc[0])
        return v if np.isfinite(v) else 0.0
    return 0.0

def get_sp_z(team: str, date_val: str) -> float:
    if {"date","team"}.issubset(sp_df.columns):
        m = (sp_df["team"] == team) & (sp_df["date"] == date_val)
        if m.any():
            v = float(sp_df.loc[m, "sp_strength_z"].iloc[0])
            return v if np.isfinite(v) else 0.0
    # fallback without date
    m2 = (sp_df.get("team","") == team)
    if m2.any():
        v = float(sp_df.loc[m2, "sp_strength_z"].iloc[0])
        return v if np.isfinite(v) else 0.0
    return 0.0

# ---------------- Build projections (anchored to games list) ----------------
out_rows = []
for _, g in games.iterrows():
    date_val = str(g.get("date",""))
    gid      = str(g.get("game_id","")) if "game_id" in games.columns else ""
    home     = norm_team(str(g.get("home_team","")))
    away     = norm_team(str(g.get("away_team","")))

    home_off = get_offense_z(home, away, date_val)
    away_off = get_offense_z(away, home, date_val)

    home_opp_sp = get_sp_z(away, date_val)  # away SP vs home bats
    away_opp_sp = get_sp_z(home, date_val)  # home SP vs away bats

    mu_home = BASELINE + ALPHA*home_off - BETA*home_opp_sp
    mu_away = BASELINE + ALPHA*away_off - BETA*away_opp_sp
    mu_home = max(0.0, float(mu_home)) if np.isfinite(mu_home) else 0.0
    mu_away = max(0.0, float(mu_away)) if np.isfinite(mu_away) else 0.0

    out_rows.append({
        "date": date_val,
        "game_id": gid,
        "home_team": home,
        "away_team": away,
        "proj_home_runs": round(mu_home, 3),
        "proj_away_runs": round(mu_away, 3),
        "proj_total": round(mu_home + mu_away, 3),
        "favorite_correct": "",
        "actual_real_run_total": "",
        "run_total_diff": "",
        "home_score": "",
        "away_score": "",
    })

out = pd.DataFrame(out_rows, columns=[
    "date","game_id","home_team","away_team",
    "proj_home_runs","proj_away_runs","proj_total",
    "favorite_correct","actual_real_run_total","run_total_diff",
    "home_score","away_score"
])

# ---------------- Write ----------------
ensure_dir(OUTFILE)
out.to_csv(OUTFILE, index=False, encoding="utf-8", lineterminator="\n")
print(f"Wrote {len(out)} rows -> {OUTFILE}")
