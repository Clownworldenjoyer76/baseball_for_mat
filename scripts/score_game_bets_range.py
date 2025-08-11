#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
score_game_bets_range.py (API-enabled, bet_history-locked)

Reads scores from MLB Stats API and updates ONLY these columns (only when blank):
  - home_score
  - away_score
  - actual_real_run_total
  - run_total_diff
  - favorite_correct

I/O is restricted to: data/bets/bet_history
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import requests

# ---- CONSTANTS ----
BET_HISTORY_DIR = Path("data/bets/bet_history").resolve()
TARGET_COLS = [
    "home_score",
    "away_score",
    "actual_real_run_total",
    "run_total_diff",
    "favorite_correct",
]

# Optional simple name normalizer (extend as needed)
TEAM_ALIASES: Dict[str, str] = {
    "athletics": "oakland athletics",
}

# ----------------- helpers -----------------
def _is_empty(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float):
        return pd.isna(x)
    if isinstance(x, str):
        return x.strip() == ""
    return False

def _norm_header_map(df: pd.DataFrame) -> Dict[str, str]:
    return {c.lower().strip(): c for c in df.columns}

def _find_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    m = _norm_header_map(df)
    for name in candidates:
        k = name.lower().strip()
        if k in m:
            return m[k]
    return None

def _norm_name(s: object) -> str:
    base = (
        str(s)
        .strip()
        .lower()
        .replace(".", "")
        .replace(",", "")
        .replace("-", " ")
    )
    base = " ".join(base.split())
    return TEAM_ALIASES.get(base, base)

def _winner_side(df: pd.DataFrame, home_score_col: str, away_score_col: str) -> pd.Series:
    hs = pd.to_numeric(df[home_score_col], errors="coerce")
    as_ = pd.to_numeric(df[away_score_col], errors="coerce")
    out = pd.Series(index=df.index, dtype=object)
    out[(hs > as_)] = "home"
    out[(as_ > hs)] = "away"
    out[(hs == as_)] = "push"
    return out

def _favorite_side(df: pd.DataFrame) -> pd.Series:
    """Return Series with values 'home', 'away', or NaN if unknown."""
    home_col = _find_col(df, ["home_team", "home"])
    away_col = _find_col(df, ["away_team", "away", "visitor", "visitorteam"])
    fav_side_col = _find_col(df, ["favorite_side", "fav_side", "side_favorite"])
    fav_team_col = _find_col(df, ["favorite_team", "ml_favorite_team", "closing_favorite_team"])

    home_ml = _find_col(df, ["home_ml", "home_moneyline", "moneyline_home"])
    away_ml = _find_col(df, ["away_ml", "away_moneyline", "moneyline_away"])
    home_spread = _find_col(df, ["home_spread", "home_handicap", "spread_home"])
    away_spread = _find_col(df, ["away_spread", "away_handicap", "spread_away"])

    if fav_side_col:
        s = df[fav_side_col].astype(str).str.strip().str.lower()
        return s.replace({"h": "home", "a": "away", "home_team": "home", "away_team": "away"})

    if fav_team_col and home_col and away_col:
        fav_norm = df[fav_team_col].apply(_norm_name)
        home_norm = df[home_col].apply(_norm_name)
        away_norm = df[away_col].apply(_norm_name)
        side = pd.Series(index=df.index, dtype=object)
        side[fav_norm == home_norm] = "home"
        side[fav_norm == away_norm] = "away"
        return side

    if home_ml and away_ml:
        h = pd.to_numeric(df[home_ml], errors="coerce")
        a = pd.to_numeric(df[away_ml], errors="coerce")
        side = pd.Series(index=df.index, dtype=object)
        side[(h.notna()) & (a.notna()) & (h < a)] = "home"
        side[(h.notna()) & (a.notna()) & (a < h)] = "away"
        return side

    if home_spread and away_spread:
        hs = pd.to_numeric(df[home_spread], errors="coerce")
        as_ = pd.to_numeric(df[away_spread], errors="coerce")
        side = pd.Series(index=df.index, dtype=object)
        side[(hs.notna()) & (as_.notna()) & (hs < as_)] = "home"
        side[(hs.notna()) & (as_.notna()) & (as_ < hs)] = "away"
        return side

    return pd.Series(index=df.index, dtype=object)

# --------------- MLB API ---------------
def fetch_scores_from_api(api_base: str, date_str: str) -> Dict[Tuple[str, str], Tuple[int, int]]:
    """
    Return {(home_norm, away_norm): (home_runs, away_runs)} for the given date.
    Only includes games with numeric runs available.
    """
    url = f"{api_base.rstrip('/')}/schedule"
    params = {
        "sportId": 1,
        "date": date_str,
        "language": "en",
        "hydrate": "linescore",
        "expand": "schedule.linescore,schedule.teams",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    out: Dict[Tuple[str, str], Tuple[int, int]] = {}
    for d in data.get("dates", []):
        for g in d.get("games", []):
            teams = g.get("teams", {})
            home_team_name = teams.get("home", {}).get("team", {}).get("name")
            away_team_name = teams.get("away", {}).get("team", {}).get("name")
            ls = g.get("linescore", {}) or {}
            home_runs = ls.get("teams", {}).get("home", {}).get("runs")
            away_runs = ls.get("teams", {}).get("away", {}).get("runs")
            if home_team_name and away_team_name and home_runs is not None and away_runs is not None:
                key = (_norm_name(home_team_name), _norm_name(away_team_name))
                try:
                    out[key] = (int(home_runs), int(away_runs))
                except Exception:
                    pass
    return out

# ----------------- main work -----------------
def process(file_in: Path, file_out: Path | None, api_base: str, date_str: str, check: bool) -> int:
    # Enforce bet_history dir
    file_in = file_in.resolve()
    if BET_HISTORY_DIR not in file_in.parents:
        raise ValueError(f"Input must be inside {BET_HISTORY_DIR}")
    if file_out:
        file_out = Path(file_out).resolve()
        if BET_HISTORY_DIR not in file_out.parents:
            raise ValueError(f"Output must be inside {BET_HISTORY_DIR}")

    df = pd.read_csv(file_in)

    # Ensure target columns exist
    for col in TARGET_COLS:
        if col not in df.columns:
            df[col] = np.nan

    home_team_col = _find_col(df, ["home_team", "home"])
    away_team_col = _find_col(df, ["away_team", "away", "visitor", "visitorteam"])
    if not home_team_col or not away_team_col:
        raise ValueError("CSV must have home_team and away_team columns (case-insensitive).")

    # API scores map
    scores_map = fetch_scores_from_api(api_base, date_str)

    def api_tuple(row):
        return scores_map.get((_norm_name(row[home_team_col]), _norm_name(row[away_team_col])))

    api_scores = df.apply(api_tuple, axis=1)

    # Fill home_score / away_score only where blank
    if not check:
        mask_home_blank = df["home_score"].apply(_is_empty)
        df.loc[mask_home_blank & api_scores.notna(), "home_score"] = (
            api_scores[mask_home_blank & api_scores.notna()].apply(lambda x: x[0]).astype("Int64")
        )

        mask_away_blank = df["away_score"].apply(_is_empty)
        df.loc[mask_away_blank & api_scores.notna(), "away_score"] = (
            api_scores[mask_away_blank & api_scores.notna()].apply(lambda x: x[1]).astype("Int64")
        )

    # Recompute helpers
    hs_num = pd.to_numeric(df["home_score"], errors="coerce").fillna(0)
    as_num = pd.to_numeric(df["away_score"], errors="coerce").fillna(0)

    # actual_real_run_total
    need_total = df["actual_real_run_total"].apply(_is_empty)
    if not check:
        df.loc[need_total, "actual_real_run_total"] = (hs_num + as_num)[need_total].astype("Int64")

    # run_total_diff
    total_col = _find_col(df, ["total", "closing_total", "market_total", "ou_total", "line_total", "game_total"])
    if total_col:
        market = pd.to_numeric(df[total_col], errors="coerce")
        actual = pd.to_numeric(df["actual_real_run_total"], errors="coerce")
        need_diff = df["run_total_diff"].apply(_is_empty)
        mask = need_diff & market.notna() & actual.notna()
        if not check:
            df.loc[mask, "run_total_diff"] = (market[mask] - actual[mask]).abs()

    # favorite_correct
    fav_side = _favorite_side(df)
    winner = _winner_side(df, "home_score", "away_score")
    need_fc = df["favorite_correct"].apply(_is_empty)
    mask = need_fc & fav_side.isin(["home", "away"]) & winner.isin(["home", "away"])
    if not check:
        df.loc[mask, "favorite_correct"] = (fav_side[mask] == winner[mask]).astype(int)

    if check:
        print("[DRY-RUN] Completed checks. No changes written.")
        return 0

    out = file_out or file_in
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Wrote updates to: {out}")
    return 0

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (used for API query and default filename)")
    ap.add_argument("--file", help="Explicit filename inside bet_history, e.g., game_bets_2025-08-11.csv")
    ap.add_argument("--output", help="Optional output filename (must be inside bet_history). Defaults to in-place.")
    ap.add_argument("--api", default="https://statsapi.mlb.com/api/v1", help="MLB Stats API base URL")
    ap.add_argument("--check", action="store_true", help="Dry-run without writing changes")
    args = ap.parse_args()

    if not args.date and not args.file:
        raise SystemExit("Provide --date OR --file")

    if args.file:
        file_in = BET_HISTORY_DIR / args.file
        # If no --date, try to infer from filename suffix _YYYY-MM-DD.csv
        date_str = args.date
        if not date_str:
            stem = Path(args.file).stem
            parts = stem.split("_")
            date_str = parts[-1] if parts else ""
            if len(date_str) != 10:
                raise SystemExit("Provide --date when filename does not include a YYYY-MM-DD suffix.")
    else:
        date_str = args.date
        file_in = BET_HISTORY_DIR / f"game_bets_{date_str}.csv"

    file_out = (BET_HISTORY_DIR / args.output) if args.output else None
    return process(file_in, file_out, args.api, date_str, args.check)

if __name__ == "__main__":
    raise SystemExit(main())
