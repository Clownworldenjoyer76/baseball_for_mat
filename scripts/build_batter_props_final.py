#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build the final batter props file with robust team normalization.

Fixes the issue where schedule lines like "St. Louis Cardinals" were not
recognized because downstream logic expects aliases like "Cardinals".
This script normalizes ALL team fields across:
  - data/bets/prep/batter_props_bets.csv
  - data/bets/mlb_sched.csv
  - data/bets/prep/pitcher_props_bets.csv
  - data/_projections/batter_props_projected.csv  (optional)

If the projections file exists, it's merged on ['player','market'] when possible.
Otherwise we pass through the bets after normalization.

Outputs:
  - data/bets/prep/batter_props_final.csv
"""

from pathlib import Path
import math
import sys
import pandas as pd
import numpy as np

# ---------------- paths ----------------
BATTER_IN   = Path("data/bets/prep/batter_props_bets.csv")
SCHED_IN    = Path("data/bets/mlb_sched.csv")
PITCHER_IN  = Path("data/bets/prep/pitcher_props_bets.csv")
PROJ_IN     = Path("data/_projections/batter_props_projected.csv")  # optional
OUT_FILE    = Path("data/bets/prep/batter_props_final.csv")

# ---------------- helpers ----------------
def _norm_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _to_scalar(x):
    """Return a single scalar from possible Series/array/list/scalar; NaN if empty."""
    if isinstance(x, pd.Series):
        x = x.dropna()
        return x.iloc[0] if len(x) else np.nan
    if isinstance(x, (list, tuple, np.ndarray)):
        arr = [y for y in x if not (isinstance(y, float) and math.isnan(y))]
        return arr[0] if arr else np.nan
    return x

# ---------------- team normalization ----------------
# Canonical aliases set (what downstream expects)
ALIAS_SET = {
    "Diamondbacks","Braves","Orioles","Red Sox","Cubs","White Sox","Reds","Guardians",
    "Rockies","Tigers","Astros","Royals","Angels","Dodgers","Marlins","Brewers",
    "Twins","Mets","Yankees","Athletics","Phillies","Pirates","Padres","Giants",
    "Mariners","Cardinals","Rays","Rangers","Blue Jays","Nationals"
}

# Primary dictionary of common full-name -> alias mappings
TEAM_ALIASES = {
    # NL West / Central / East
    "arizona diamondbacks":"Diamondbacks",
    "atlanta braves":"Braves",
    "chicago cubs":"Cubs",
    "cincinnati reds":"Reds",
    "colorado rockies":"Rockies",
    "los angeles dodgers":"Dodgers",
    "miami marlins":"Marlins",
    "milwaukee brewers":"Brewers",
    "new york mets":"Mets",
    "philadelphia phillies":"Phillies",
    "pittsburgh pirates":"Pirates",
    "san diego padres":"Padres",
    "san francisco giants":"Giants",
    "st. louis cardinals":"Cardinals",
    "st louis cardinals":"Cardinals",
    # AL
    "baltimore orioles":"Orioles",
    "boston red sox":"Red Sox",
    "chicago white sox":"White Sox",
    "cleveland guardians":"Guardians",
    "detroit tigers":"Tigers",
    "houston astros":"Astros",
    "kansas city royals":"Royals",
    "los angeles angels":"Angels",
    "la angels":"Angels",
    "minnesota twins":"Twins",
    "new york yankees":"Yankees",
    "oakland athletics":"Athletics",
    "seattle mariners":"Mariners",
    "tampa bay rays":"Rays",
    "texas rangers":"Rangers",
    "toronto blue jays":"Blue Jays",
    "washington nationals":"Nationals",
    # Common sportsbook/CSV variants
    "la dodgers":"Dodgers",
    "san diego friars":"Padres",
    "ny mets":"Mets",
    "ny yankees":"Yankees",
    "chi cubs":"Cubs",
    "chi white sox":"White Sox",
    "sf giants":"Giants",
    "sd padres":"Padres",
    "st louis":"Cardinals",
    "st. louis":"Cardinals",
    "tampa bay":"Rays",
    "arizona":"Diamondbacks",
    "kansas city":"Royals",
    "oakland a's":"Athletics",
    "oakland as":"Athletics",
    "la angels of anaheim":"Angels",
    "los angeles angels of anaheim":"Angels",
}

def _preclean(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    out = str(s).strip().lower()
    # light punctuation unification
    out = out.replace(".", "").replace(",", "")
    out = " ".join(out.split())
    return out

def normalize_team(value: str) -> str:
    """
    Convert any reasonable team string to canonical alias (e.g., 'Cardinals').
    Fallback heuristic: if string has multiple words and the last word matches
    a known alias, use that; else return original trimmed value.
    """
    raw = str(value).strip()
    key = _preclean(value)

    if key in TEAM_ALIASES:
        return TEAM_ALIASES[key]

    # If already a clean alias, keep it
    if raw in ALIAS_SET:
        return raw

    # Heuristic: many inputs are "City Nickname" -> take last token if it is an alias
    toks = key.split()
    if toks:
        last = toks[-1].capitalize()
        if last in ALIAS_SET:
            return last

    # Final fallback: title-case but unchanged (so we don't lose info)
    return raw

def normalize_team_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize any columns that look like team identifiers.
    """
    if df is None or df.empty:
        return df
    cols = [c for c in df.columns if any(tok in c.lower() for tok in ["team","opponent","opp","home","away"])]
    for c in cols:
        df[c] = df[c].map(normalize_team)
    return df

def validate_teams(df: pd.DataFrame, cols: list, context: str):
    """
    Validate that given columns only include known aliases.
    Raise with a helpful message if not.
    """
    unknown = {}
    for c in cols:
        if c in df.columns:
            bad = sorted(set(x for x in df[c].dropna().unique() if x not in ALIAS_SET))
            if bad:
                unknown[c] = bad
    if unknown:
        msg_lines = [f"Unknown team alias(es) detected in {context}:"]
        for k, v in unknown.items():
            msg_lines.append(f"  {k}: {v}")
        raise ValueError("\n".join(msg_lines))

# ---------------- main ----------------
def main():
    # ---- Load inputs (some may be optional) ----
    if not BATTER_IN.exists():
        raise FileNotFoundError(f"Missing required file: {BATTER_IN}")
    if not SCHED_IN.exists():
        raise FileNotFoundError(f"Missing required file: {SCHED_IN}")

    batter = pd.read_csv(BATTER_IN)
    sched  = pd.read_csv(SCHED_IN)

    pitcher = pd.DataFrame()
    if PITCHER_IN.exists():
        pitcher = pd.read_csv(PITCHER_IN)

    proj = pd.DataFrame()
    if PROJ_IN.exists():
        proj = pd.read_csv(PROJ_IN)

    # ---- Normalize teams everywhere ----
    batter = normalize_team_cols(batter)
    sched  = normalize_team_cols(sched)
    pitcher = normalize_team_cols(pitcher) if not pitcher.empty else pitcher
    proj    = normalize_team_cols(proj) if not proj.empty else proj

    # ---- Validate schedule (primary source of truth for team strings) ----
    sched_team_cols = [c for c in ["home_team","away_team","team","opponent","opp_team"] if c in sched.columns]
    validate_teams(sched, sched_team_cols, context="schedule")

    # (Optional) Validate batter & pitcher files too, but keep them permissive:
    batter_team_cols = [c for c in ["team","opponent","opp","opp_team"] if c in batter.columns]
    try:
        validate_teams(batter, batter_team_cols, context="batter props")
    except ValueError as e:
        # Make this non-fatal: print warning to stderr; continue after normalization
        sys.stderr.write(str(e) + "\n")

    if not pitcher.empty:
        pitcher_team_cols = [c for c in ["team","opponent","opp","opp_team"] if c in pitcher.columns]
        try:
            validate_teams(pitcher, pitcher_team_cols, context="pitcher props")
        except ValueError as e:
            sys.stderr.write(str(e) + "\n")

    # ---- Attach projections if present (best-effort) ----
    out = batter.copy()
    if not proj.empty:
        # Standardize likely key columns
        for df in (out, proj):
            for col in ["player","Player","PLAYER"]:
                if col in df.columns:
                    df.rename(columns={col: "player"}, inplace=True)
            for col in ["market","Market","MARKET","prop_market"]:
                if col in df.columns:
                    df.rename(columns={col: "market"}, inplace=True)

        merge_keys = [k for k in ["player","market"] if k in out.columns and k in proj.columns]
        if merge_keys:
            proj_cols = [c for c in proj.columns if c not in merge_keys]
            out = out.merge(proj[merge_keys + proj_cols], on=merge_keys, how="left")

    # ---- Final tidy/ordering (keep original columns first) ----
    # Ensure consistent column order with any new columns appended at the end
    final_cols = list(out.columns)
    out = out[final_cols]

    # ---- Write output ----
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(out):,} rows to {OUT_FILE}")

if __name__ == "__main__":
    main()
