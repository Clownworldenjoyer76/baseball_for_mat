#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
project_batter_props.py
- Normalize team names across inputs (schedule, bets, pitcher props, projections).
- Guarantee a projections file exists for downstream steps.
This prevents failures like:
  "Unknown team alias(es) in schedule 'away_team': ['St. Louis Cardinals']"
"""

from pathlib import Path
import sys
import math
import pandas as pd
import numpy as np

# ---------- Paths ----------
BATTER_IN   = Path("data/bets/prep/batter_props_bets.csv")
SCHED_IN    = Path("data/bets/mlb_sched.csv")
PITCHER_IN  = Path("data/bets/prep/pitcher_props_bets.csv")
PROJ_OUT    = Path("data/_projections/batter_props_projected.csv")  # produced/ensured here

# ---------- Helpers ----------
ALIAS_SET = {
    "Diamondbacks","Braves","Orioles","Red Sox","Cubs","White Sox","Reds","Guardians",
    "Rockies","Tigers","Astros","Royals","Angels","Dodgers","Marlins","Brewers",
    "Twins","Mets","Yankees","Athletics","Phillies","Pirates","Padres","Giants",
    "Mariners","Cardinals","Rays","Rangers","Blue Jays","Nationals"
}

TEAM_ALIASES = {
    # NL / AL full-name variants
    "arizona diamondbacks":"Diamondbacks",
    "atlanta braves":"Braves",
    "baltimore orioles":"Orioles",
    "boston red sox":"Red Sox",
    "chicago cubs":"Cubs",
    "chicago white sox":"White Sox",
    "cincinnati reds":"Reds",
    "cleveland guardians":"Guardians",
    "colorado rockies":"Rockies",
    "detroit tigers":"Tigers",
    "houston astros":"Astros",
    "kansas city royals":"Royals",
    "los angeles angels":"Angels",
    "los angeles angels of anaheim":"Angels",
    "la angels of anaheim":"Angels",
    "los angeles dodgers":"Dodgers",
    "miami marlins":"Marlins",
    "milwaukee brewers":"Brewers",
    "minnesota twins":"Twins",
    "new york mets":"Mets",
    "new york yankees":"Yankees",
    "oakland athletics":"Athletics",
    "philadelphia phillies":"Phillies",
    "pittsburgh pirates":"Pirates",
    "san diego padres":"Padres",
    "san francisco giants":"Giants",
    "seattle mariners":"Mariners",
    "tampa bay rays":"Rays",
    "texas rangers":"Rangers",
    "toronto blue jays":"Blue Jays",
    "washington nationals":"Nationals",
    # Common short variants
    "la dodgers":"Dodgers",
    "ny mets":"Mets",
    "ny yankees":"Yankees",
    "chi cubs":"Cubs",
    "chi white sox":"White Sox",
    "sf giants":"Giants",
    "sd padres":"Padres",
    "st. louis cardinals":"Cardinals",
    "st louis cardinals":"Cardinals",
    "st. louis":"Cardinals",
    "st louis":"Cardinals",
    "tampa bay":"Rays",
    "arizona":"Diamondbacks",
    "kansas city":"Royals",
    "oakland as":"Athletics",
    "oakland a's":"Athletics",
}

def _preclean(s):
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    out = str(s).strip().lower()
    out = out.replace(".", "").replace(",", "")
    out = " ".join(out.split())
    return out

def normalize_team(x: str) -> str:
    raw = str(x).strip()
    key = _preclean(x)
    if key in TEAM_ALIASES:
        return TEAM_ALIASES[key]
    if raw in ALIAS_SET:
        return raw
    toks = key.split()
    if toks:
        last = toks[-1].capitalize()
        if last in ALIAS_SET:
            return last
    return raw  # leave unchanged if we can't confidently map

def normalize_team_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = [c for c in df.columns if any(tok in c.lower() for tok in ["team","opponent","opp","home","away"])]
    for c in cols:
        df[c] = df[c].map(normalize_team)
    return df

def validate_teams(df: pd.DataFrame, cols: list, context: str):
    unknown = {}
    for c in cols:
        if c in df.columns:
            bad = sorted(set(x for x in df[c].dropna().unique() if x not in ALIAS_SET))
            if bad:
                unknown[c] = bad
    if unknown:
        lines = [f"Unknown team alias(es) detected in {context}:"]
        for k, v in unknown.items():
            lines.append(f"  {k}: {v}")
        raise ValueError("\n".join(lines))

def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

# ---------- Main ----------
def main():
    if not SCHED_IN.exists():
        raise FileNotFoundError(f"Missing required schedule file: {SCHED_IN}")
    if not BATTER_IN.exists():
        raise FileNotFoundError(f"Missing required bets file: {BATTER_IN}")

    # Load
    sched = pd.read_csv(SCHED_IN)
    bets  = pd.read_csv(BATTER_IN)
    pitcher = pd.read_csv(PITCHER_IN) if PITCHER_IN.exists() else pd.DataFrame()

    # Normalize teams everywhere
    sched  = normalize_team_cols(sched)
    bets   = normalize_team_cols(bets)
    pitcher = normalize_team_cols(pitcher) if not pitcher.empty else pitcher

    # Validate schedule only (source of truth for team strings)
    sched_cols = [c for c in ["home_team","away_team","team","opponent","opp_team"] if c in sched.columns]
    validate_teams(sched, sched_cols, "schedule")

    # Overwrite normalized schedule back to disk (so downstream uses fixed names)
    sched.to_csv(SCHED_IN, index=False)

    # Ensure a projections file exists; if not, create a basic pass-through from bets
    if PROJ_OUT.exists():
        # Still rewrite a normalized version if needed
        proj = pd.read_csv(PROJ_OUT)
        proj = normalize_team_cols(proj)
        ensure_parent(PROJ_OUT)
        proj.to_csv(PROJ_OUT, index=False)
    else:
        # Minimal projection: keep key identifiers; add placeholder projection columns if missing
        df = bets.copy()
        # Standardize likely key names
        rename_map = {}
        for col in ["player","Player","PLAYER"]:
            if col in df.columns:
                rename_map[col] = "player"
                break
        for col in ["market","Market","MARKET","prop_market"]:
            if col in df.columns:
                rename_map[col] = "market"
                break
        if rename_map:
            df = df.rename(columns=rename_map)

        # Add basic projection columns if not present
        if "projected_value" not in df.columns:
            df["projected_value"] = np.nan
        if "projected_prob" not in df.columns:
            df["projected_prob"] = np.nan

        ensure_parent(PROJ_OUT)
        df.to_csv(PROJ_OUT, index=False)

    print("Team normalization complete; schedule and projections ensured.")
    print(f" - Normalized schedule saved: {SCHED_IN}")
    print(f" - Projections available at: {PROJ_OUT}")

if __name__ == "__main__":
    main()
