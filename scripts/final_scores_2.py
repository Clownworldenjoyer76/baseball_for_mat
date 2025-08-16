#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Projects per-game expected runs using ONLY:
  - data/bets/prep/batter_props_final.csv
  - data/bets/prep/pitcher_props_bets.csv  (loaded for parity; not required if batter file has opp_pitcher_z)
  - data/raw/todaysgames_normalized.csv     (drives game list + real home/away)

Writes:
  - data/bets/game_props_history.csv

Intentionally leaves BLANK:
  favorite_correct, actual_real_run_total, run_total_diff, home_score, away_score
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
import re

# ---------------- Paths ----------------
BATTERS = Path("data/bets/prep/batter_props_final.csv")
PITCHERS = Path("data/bets/prep/pitcher_props_bets.csv")
GAMES   = Path("data/raw/todaysgames_normalized.csv")
OUTFILE = Path("data/bets/game_props_history.csv")

# ---------------- Simple model constants (tune later if desired) ----------------
BASELINE = 4.5      # league-average runs per team per game
ALPHA    = 0.8      # lineup z impact
BETA     = 0.8      # opposing SP z impact
PROB_MIN, PROB_MAX = 0.50, 0.99  # clamp for weighting if over_probability exists

# ---------------- Helpers ----------------
def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Lower/strip columns; coerce core identifiers to clean strings; normalize date."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    for c in ("team", "opp_team", "home_team", "away_team", "game_id", "date"):
        if c in df.columns:
            df[c] = df[c].astype("string").fillna("").str.strip()
    if "date" in df.columns:
        parsed = pd.to_datetime(df["date"], errors="coerce", utc=False)
        df["date"] = parsed.dt.date.astype("string").fillna("")
    # scrub literal "nan"
    for c in ("team", "opp_team", "home_team", "away_team", "game_id", "date"):
        if c in df.columns:
            df[c] = df[c].replace({"nan": ""})
    return df

def _assert_cols(df: pd.DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing required column(s): {missing}")

def _normalize_team_strings(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Trim/collapse whitespace and normalize a few common variants."""
    mapping = {
        "d-backs": "diamondbacks",
        "dbacks": "diamondbacks",
        "ny yankees": "yankees",
        "st. louis cardinals": "cardinals",
        "la angels": "angels",
        "los angeles angels": "angels",
        "oakland athletics": "athletics",
        "chi white sox": "white sox",
        "chi cubs": "cubs",
        "sd padres": "padres",
        "sf giants": "giants",
        "tb rays": "rays",
        "tampa bay devil rays": "rays",
    }
    for c in cols:
        if c in df.columns:
            s = df[c].astype("string").fillna("").str.strip()
            s = s.str.replace(r"\s+", " ", regex=True)
            # normalize punctuation and case for mapping, then restore case title-ish
            s_lower = s.str.lower()
            for k, v in mapping.items():
                s_lower = s_lower.str.replace(re.escape(k), v, regex=True)
            df[c] = s_lower.str.title()
    return df

def _wmean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    m = v.notna() & w.notna()
    return float(np.average(v[m], weights=w[m])) if m.any() else np.nan

def _smean(values: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    return float(v.mean()) if v.notna().any() else np.nan

def _ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def _parse_matchup_to_home_away(df: pd.DataFrame) -> pd.DataFrame:
    """If games file lacks home_team/away_team, try to parse from 'matchup' like 'Away @ Home'."""
    if "matchup" not in df.columns:
        return df
    away_list, home_list = [], []
    for s in df["matchup"].astype("string").fillna(""):
        parts = re.split(r"@|\bat\b", s)
        if len(parts) >= 2:
            away = parts[0].strip()
            home = parts[1].strip()
        else:
            away, home = "", ""
        away_list.append(away)
        home_list.append(home)
    if "away_team" not in df.columns:
        df["away_team"] = pd.Series(away_list, dtype="string")
    if "home_team" not in df.columns:
        df["home_team"] = pd.Series(home_list, dtype="string")
    return df

# ---------------- Load inputs ----------------
if not BATTERS.exists():
    raise SystemExit(f"Missing input: {BATTERS}")
if not PITCHERS.exists():
    raise SystemExit(f"Missing input: {PITCHERS}")
if not GAMES.exists():
    raise SystemExit(f"Missing input: {GAMES}")

bat = _prep(pd.read_csv(BATTERS))
_   = _prep(pd.read_csv(PITCHERS))  # kept for parity; not strictly used if batter file has opp_pitcher_z
games = _prep(pd.read_csv(GAMES))

# ---------------- Prepare games (authoritative home/away/date/game_id) ----------------
games = _parse_matchup_to_home_away(games)

# Allow several common column name variants
# date
if "date" not in games.columns:
    # try game_date or similar
    for alt in ["game_date", "gamedate"]:
        if alt in games.columns:
            games.rename(columns={alt: "date"}, inplace=True)
            break
    games["date"] = games.get("date", pd.Series("", index=games.index, dtype="string"))
# home/away
if "home_team" not in games.columns or "away_team" not in games.columns:
    raise SystemExit("todaysgames_normalized.csv must contain home_team and away_team (or a parsable 'matchup').")
# game_id (optional but helpful)
if "game_id" not in games.columns:
    # synthesize stable id
    games["game_id"] = (
        games["home_team"].str[:3].str.upper() + "_" +
        games["away_team"].str[:3].str.upper() + "_" +
        pd.Series(range(1, len(games) + 1), dtype="int").astype(str)
    )

# Normalize team names to improve matching
games = _normalize_team_strings(games, ["home_team", "away_team"])
for c in ("home_team", "away_team", "date", "game_id"):
    games[c] = games[c].astype("string").fillna("").str.strip()

# Reduce to minimal set we need
games = games[["date", "game_id", "home_team", "away_team"]].drop_duplicates().reset_index(drop=True)

# ---------------- Validate / normalize batter props ----------------
# Required: team & opp_team to compute directional strengths
required_bat_cols = ["team", "opp_team"]
missing = [c for c in required_bat_cols if c not in bat.columns]
if missing:
    raise SystemExit(f"batter_props_final.csv missing required columns: {missing}")

# Strength column: prefer mega_z, then batter_z, else zeros
if "mega_z" in bat.columns:
    strength_col = "mega_z"
elif "batter_z" in bat.columns:
    strength_col = "batter_z"
else:
    strength_col = "_zero_strength"
    bat[strength_col] = 0.0

# Weights from over_probability if present
if "over_probability" in bat.columns:
    op = pd.to_numeric(bat["over_probability"], errors="coerce").clip(0.0, 1.0)
    bat["__weight"] = op.fillna(0.75).clip(PROB_MIN, PROB_MAX)
else:
    bat["__weight"] = 1.0

# Opposing starter z from batter rows if available
opp_sp_col = "opp_pitcher_z" if "opp_pitcher_z" in bat.columns else None

# Normalize team strings in batters
bat = _normalize_team_strings(bat, ["team", "opp_team"])
for c in ("team", "opp_team", "date", "game_id"):
    if c in bat.columns:
        bat[c] = bat[c].astype("string").fillna("").str.strip()

# ---------------- Aggregate batter props to team-vs-opp rows ----------------
group_keys = [k for k in ["date", "game_id", "team", "opp_team"] if k in bat.columns]
if not group_keys:
    group_keys = ["team", "opp_team"]

agg_rows = []
for keys, df in bat.groupby(group_keys, dropna=False):
    if not isinstance(keys, tuple):
        keys = (keys,)
    kdict = dict(zip(group_keys, [str(k) for k in keys]))

    off_strength = _wmean(df[strength_col], df["__weight"])
    opp_sp_z     = _smean(df[opp_sp_col]) if opp_sp_col and opp_sp_col in df.columns else np.nan

    agg_rows.append({
        **kdict,
        "offense_strength_z": off_strength,
        "opp_sp_strength_z": opp_sp_z
    })

team_vs_opp = pd.DataFrame(agg_rows)
for c in ("team", "opp_team", "date", "game_id"):
    if c in team_vs_opp.columns:
        team_vs_opp[c] = team_vs_opp[c].astype("string").fillna("").str.strip()

# Helper to fetch a directional strength (Team A vs Team B), first try same date, then any date fallback
def _get_strength(a_team: str, b_team: str, date_val: str) -> tuple[float, float]:
    """Return (offense_strength_z, opp_sp_strength_z) for a_team vs b_team."""
    # Date-aware exact
    mask = (team_vs_opp.get("team", "") == a_team) & (team_vs_opp.get("opp_team", "") == b_team)
    if "date" in team_vs_opp.columns and date_val:
        mask = mask & (team_vs_opp.get("date", "") == date_val)
    rows = team_vs_opp.loc[mask]
    if not rows.empty:
        return (
            float(rows["offense_strength_z"].iloc[0]) if pd.notna(rows["offense_strength_z"].iloc[0]) else 0.0,
            float(rows["opp_sp_strength_z"].iloc[0]) if pd.notna(rows["opp_sp_strength_z"].iloc[0]) else 0.0,
        )
    # Fallback: ignore date
    mask2 = (team_vs_opp.get("team", "") == a_team) & (team_vs_opp.get("opp_team", "") == b_team)
    rows2 = team_vs_opp.loc[mask2]
    if not rows2.empty:
        return (
            float(rows2["offense_strength_z"].iloc[0]) if pd.notna(rows2["offense_strength_z"].iloc[0]) else 0.0,
            float(rows2["opp_sp_strength_z"].iloc[0]) if pd.notna(rows2["opp_sp_strength_z"].iloc[0]) else 0.0,
        )
    # Nothing found
    return 0.0, 0.0

# ---------------- Build projections anchored to games file ----------------
out_rows = []
for _, g in games.iterrows():
    date_val = str(g["date"])
    gid_val  = str(g["game_id"])
    home     = str(g["home_team"])
    away     = str(g["away_team"])

    # Get directional strengths
    # Home offense vs Away pitcher
    home_off_z, home_opp_sp_z = _get_strength(home, away, date_val)
    # Away offense vs Home pitcher
    away_off_z, away_opp_sp_z = _get_strength(away, home, date_val)

    # μ = BASELINE + ALPHA*offense_z − BETA*opp_SP_z
    mu_home = BASELINE + ALPHA * home_off_z - BETA * home_opp_sp_z
    mu_away = BASELINE + ALPHA * away_off_z - BETA * away_opp_sp_z
    mu_home = max(0.0, float(mu_home)) if np.isfinite(mu_home) else 0.0
    mu_away = max(0.0, float(mu_away)) if np.isfinite(mu_away) else 0.0

    out_rows.append({
        "date": date_val,
        "game_id": gid_val,
        "home_team": home,
        "away_team": away,
        "proj_home_runs": round(mu_home, 3),
        "proj_away_runs": round(mu_away, 3),
        "proj_total": round(mu_home + mu_away, 3),
    })

out = pd.DataFrame(out_rows, columns=[
    "date", "game_id", "home_team", "away_team",
    "proj_home_runs", "proj_away_runs", "proj_total"
])

# ---------------- Fail-fast smoke checks ----------------
if out.empty:
    raise SystemExit("No games produced—verify todaysgames_normalized.csv has home_team/away_team (and date/game_id if available).")
assert {"home_team", "away_team"}.issubset(out.columns), "Missing home/away in output."
assert out[["proj_home_runs", "proj_away_runs"]].notna().all().all(), "Found NaNs in projections."
assert (out["proj_home_runs"] >= 0).all() and (out["proj_away_runs"] >= 0).all(), "Negative projections."

# ---------------- Add required blank columns ----------------
for col in ["favorite_correct", "actual_real_run_total", "run_total_diff", "home_score", "away_score"]:
    out[col] = pd.NA

# ---------------- Write ----------------
_ensure_dir(OUTFILE)
out.to_csv(OUTFILE, index=False, encoding="utf-8", lineterminator="\n")
print(f"Wrote {len(out)} rows -> {OUTFILE}")
