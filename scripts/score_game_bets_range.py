#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
score_game_bets_range_bet_history.py
------------------------------------
Restricts ALL I/O to the folder: data/bets/bet_history

This script ONLY fills these columns (and only when currently blank):
  - home_score
  - away_score
  - actual_real_run_total
  - run_total_diff
  - favorite_correct

Rules / behavior:
- Case-insensitive detection of common source/alias columns.
- Coerces numbers safely; ignores bad/missing values.
- Never overwrites a non-empty target cell.
- favorite_correct is computed from favorite_side/favorite_team if available,
  or inferred from moneylines/spreads.
- run_total_diff = abs(market_total - actual_real_run_total) when both exist.

USAGE (I/O locked to data/bets/bet_history):
  # Date-driven (recommended). File name pattern: game_bets_YYYY-MM-DD.csv
  python score_game_bets_range_bet_history.py --date 2025-08-11

  # OR give an explicit filename that lives in the bet_history folder
  python score_game_bets_range_bet_history.py --file game_bets_2025-08-11.csv

  # Dry-run (no writes)
  python score_game_bets_range_bet_history.py --date 2025-08-11 --check

Exit codes:
  0 success
  2 error
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, List
import numpy as np
import pandas as pd

# ---- CONSTANTS ----
BET_HISTORY_DIR = Path("data/bets/bet_history").resolve()
TARGET_COLS = [
    "home_score",
    "away_score",
    "actual_real_run_total",
    "run_total_diff",
    "favorite_correct",
]


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
    """Map normalized header -> original header"""
    return {c.lower().strip(): c for c in df.columns}


def _find_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    m = _norm_header_map(df)
    for name in candidates:
        k = name.lower().strip()
        if k in m:
            return m[k]
    return None


def _norm_str(s: object) -> str:
    return (
        str(s)
        .strip()
        .lower()
        .replace(".", "")
        .replace(",", "")
        .replace("-", " ")
        .replace("  ", " ")
    )


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

    # Moneyline or spread as fallback
    home_ml = _find_col(df, ["home_ml", "home_moneyline", "moneyline_home"])
    away_ml = _find_col(df, ["away_ml", "away_moneyline", "moneyline_away"])
    home_spread = _find_col(df, ["home_spread", "home_handicap", "spread_home"])
    away_spread = _find_col(df, ["away_spread", "away_handicap", "spread_away"])

    # 1) direct side column
    if fav_side_col:
        s = df[fav_side_col].astype(str).str.strip().str.lower()
        return s.replace({"h": "home", "a": "away", "home_team": "home", "away_team": "away"})

    # 2) favorite team name -> side
    if fav_team_col and home_col and away_col:
        fav_norm = df[fav_team_col].apply(_norm_str)
        home_norm = df[home_col].apply(_norm_str)
        away_norm = df[away_col].apply(_norm_str)
        side = pd.Series(index=df.index, dtype=object)
        side[fav_norm == home_norm] = "home"
        side[fav_norm == away_norm] = "away"
        return side

    # 3) moneyline: more negative is favorite
    if home_ml and away_ml:
        h = pd.to_numeric(df[home_ml], errors="coerce")
        a = pd.to_numeric(df[away_ml], errors="coerce")
        side = pd.Series(index=df.index, dtype=object)
        side[(h.notna()) & (a.notna()) & (h < a)] = "home"
        side[(h.notna()) & (a.notna()) & (a < h)] = "away"
        return side

    # 4) spread: more negative is favorite
    if home_spread and away_spread:
        hs = pd.to_numeric(df[home_spread], errors="coerce")
        as_ = pd.to_numeric(df[away_spread], errors="coerce")
        side = pd.Series(index=df.index, dtype=object)
        side[(hs.notna()) & (as_.notna()) & (hs < as_)] = "home"
        side[(hs.notna()) & (as_.notna()) & (as_ < hs)] = "away"
        return side

    return pd.Series(index=df.index, dtype=object)  # unknown


# ----------------- main work -----------------
def process(file_in: Path, file_out: Path | None = None, check: bool = False) -> int:
    # Enforce that both input and output live under BET_HISTORY_DIR
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

    # Detect score columns (prefer existing targets, else aliases)
    home_col = _find_col(df, ["home_score", "final_home_score", "score_home", "home_runs", "home_points"]) or "home_score"
    away_col = _find_col(df, ["away_score", "final_away_score", "score_away", "away_runs", "away_points"]) or "away_score"

    # Convert existing score columns to numeric helpers
    hs_num = pd.to_numeric(df[home_col], errors="coerce")
    as_num = pd.to_numeric(df[away_col], errors="coerce")

    # 1) Fill home_score / away_score only where blank (if source columns are not the canonical ones)
    if home_col != "home_score":
        mask = df["home_score"].apply(_is_empty) & hs_num.notna()
        if not check:
            df.loc[mask, "home_score"] = hs_num[mask].astype("Int64")

    if away_col != "away_score":
        mask = df["away_score"].apply(_is_empty) & as_num.notna()
        if not check:
            df.loc[mask, "away_score"] = as_num[mask].astype("Int64")

    # Recompute helpers from the canonical columns
    hs_num = pd.to_numeric(df["home_score"], errors="coerce").fillna(0)
    as_num = pd.to_numeric(df["away_score"], errors="coerce").fillna(0)

    # 2) actual_real_run_total (only fill blanks)
    need_total = df["actual_real_run_total"].apply(_is_empty)
    if not check:
        df.loc[need_total, "actual_real_run_total"] = (hs_num + as_num)[need_total].astype("Int64")

    # 3) run_total_diff = abs(market_total - actual_total) where both exist
    total_col = _find_col(df, ["total", "closing_total", "market_total", "ou_total", "line_total", "game_total"])
    if total_col:
        market = pd.to_numeric(df[total_col], errors="coerce")
        actual = pd.to_numeric(df["actual_real_run_total"], errors="coerce")
        need_diff = df["run_total_diff"].apply(_is_empty)
        mask = need_diff & market.notna() & actual.notna()
        if not check:
            df.loc[mask, "run_total_diff"] = (market[mask] - actual[mask]).abs()

    # 4) favorite_correct (only fill blanks)
    fav_side = _favorite_side(df)  # "home"/"away"/None
    winner = _winner_side(df, "home_score", "away_score")  # "home"/"away"/"push"
    need_fc = df["favorite_correct"].apply(_is_empty)
    mask = need_fc & fav_side.isin(["home", "away"]) & winner.isin(["home", "away"])
    if not check:
        df.loc[mask, "favorite_correct"] = (fav_side[mask] == winner[mask]).astype(int)

    # Write output
    if check:
        print("[DRY-RUN] Completed checks. No changes written.")
        return 0

    out = file_out or file_in
    out.parent.mkdir(parents=True, exist_ok=True)  # ensure folder exists
    df.to_csv(out, index=False)
    print(f"Wrote updates to: {out}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD to build file name game_bets_{DATE}.csv inside bet_history")
    ap.add_argument("--file", help="Explicit filename (must be inside bet_history), e.g., game_bets_2025-08-11.csv")
    ap.add_argument("--output", help="Optional output filename (must be inside bet_history). Defaults to in-place.")
    ap.add_argument("--check", action="store_true", help="Dry-run without writing changes")
    args = ap.parse_args()

    if not args.date and not args.file:
        raise SystemExit("Provide --date OR --file")

    if args.file:
        file_in = BET_HISTORY_DIR / args.file
    else:
        file_in = BET_HISTORY_DIR / f"game_bets_{args.date}.csv"

    file_out = (BET_HISTORY_DIR / args.output) if args.output else None

    try:
        return process(file_in, file_out, args.check)
    except Exception as e:
        print(f"ERROR: {e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
