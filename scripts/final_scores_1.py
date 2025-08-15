#!/usr/bin/env python3
# scripts/final_scores_1.py
#
# Build a pure game-level table (one row per game) with a fixed schema,
# writing to data/bets/game_props_history.csv

from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Optional
import pandas as pd
import numpy as np

# ---- Inputs / Output ----
SCHED_FILE   = Path("data/bets/mlb_sched.csv")
TODAY_FILE   = Path("data/raw/todaysgames_normalized.csv")
BATTER_FILE  = Path("data/bets/prep/batter_props_bets.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")
GAME_OUT     = Path("data/bets/game_props_history.csv")

# ---- Output schema (exact order) ----
OUT_COLS: List[str] = [
    "game_id",
    "date",
    "home_team",
    "away_team",
    "venue_name",
    "favorite",
    "favorite_correct",
    "projected_real_run_total",
    "actual_real_run_total",
    "run_total_diff",
    "home_score",
    "away_score",
    "game_time",
    "pitcher_home",
    "pitcher_away",
]

# ---- Utility ----
def _std(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = d.columns.str.strip().str.lower()
    return d

def _read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def _first_present(d: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in d.columns:
            return c
    return None

def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols]

# ---- Team projection extractors ---------------------------------------------
def _team_key_cols(d: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return canonical team, opponent, and game_id column names if present."""
    team  = _first_present(d, ["team", "team_name", "team_full", "home_team", "away_team"])
    opp   = _first_present(d, ["opponent", "opp", "vs_team", "away_team", "home_team"])
    gid   = _first_present(d, ["game_id", "gamepk", "game_pk", "id"])
    return team, opp, gid

def _date_cols(d: pd.DataFrame) -> Optional[str]:
    return _first_present(d, ["date", "game_date"])

def _team_run_col(d: pd.DataFrame) -> Optional[str]:
    """
    Try to detect a single-team expected runs column in a prep file.
    Common candidates; adjust as needed for your prep outputs.
    """
    return _first_present(
        d,
        [
            "projected_team_runs",
            "team_proj_runs",
            "proj_runs",
            "expected_runs",
            "xr",
            "x_runs",
            "runs_proj",
        ],
    )

def _game_total_col(d: pd.DataFrame) -> Optional[str]:
    """Detect a game-level projected total column if one already exists."""
    return _first_present(
        d,
        [
            "projected_real_run_total",
            "projected_game_total",
            "proj_game_total",
            "game_total_proj",
            "expected_total_runs",
        ],
    )

def _safe_team_names(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _derive_game_totals_from_team_rows(d: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Attempt to derive per-game totals from per-team rows.
    Supports:
      - rows with (game_id, team, opponent)
      - or two rows per game identified by (date, home_team, away_team) style fields.
    Returns dataframe with ['game_id','date','home_team','away_team','home_proj','away_proj','projected_real_run_total'] when possible.
    """
    df = d.copy()
    date_col = _date_cols(df)
    team_col, opp_col, gid_col = _team_key_cols(df)

    # Identify a per-team projected runs number
    run_col = _team_run_col(df)
    if run_col is None:
        return None

    # Normalize strings
    for c in [team_col, opp_col]:
        if c and c in df.columns:
            df[c] = _safe_team_names(df[c])

    # Strategy A: file already has explicit home/away columns per row
    # Try to pivot/group if columns exist
    has_home = "home_team" in df.columns
    has_away = "away_team" in df.columns
    if has_home and has_away:
        keys = [c for c in ["game_id", "date", "home_team", "away_team"] if c in df.columns]
        g = (
            df.groupby(keys, dropna=False)[run_col]
              .sum()
              .reset_index(name="side_sum")
        )
        # If grouping collapsed both sides into one, we can't split; skip this path.
        # Otherwise, try to separate by team==home vs team==away if team/opponent exist.
        if team_col and opp_col and all(k in df.columns for k in ["home_team", "away_team"]):
            df["_is_home"] = (df[team_col] == df["home_team"])
            home = (df[df["_is_home"]]
                    .groupby([c for c in ["game_id", "date", "home_team", "away_team"] if c in df.columns])[run_col]
                    .sum()
                    .rename("home_proj"))
            away = (df[~df["_is_home"]]
                    .groupby([c for c in ["game_id", "date", "home_team", "away_team"] if c in df.columns])[run_col]
                    .sum()
                    .rename("away_proj"))
            out = pd.concat([home, away], axis=1).reset_index()
            if "home_proj" in out.columns and "away_proj" in out.columns:
                out["projected_real_run_total"] = out["home_proj"] + out["away_proj"]
                return out

    # Strategy B: rows are (team, opponent) pairs; build games by pairing
    if team_col and opp_col:
        cols = [c for c in [gid_col, date_col, team_col, opp_col, run_col] if c]
        sub = df[cols].copy()
        if gid_col and gid_col in sub.columns:
            # Key by game_id + normalized pairing
            sub["_key"] = sub[gid_col].astype(str).str.strip()
            # Separate home/away heuristically: prefer the row where team==home from schedule later
            # For now, produce both sides and let the outer join with schedule assign home/away.
            agg = sub.groupby([c for c in [gid_col, date_col] if c], dropna=False).apply(
                lambda g: pd.DataFrame({
                    "teams": [set(_safe_team_names(g[team_col]).tolist() + _safe_team_names(g[opp_col]).tolist())],
                    "sum_runs": [g[run_col].sum()],
                })
            ).reset_index()
            # This doesn't split sides; we need side splits to pick favorite. Fall back to later join.
            # We'll return None here; another source may provide a cleaner mapping.
            return None

    return None

def _extract_game_totals(path: Path) -> Optional[pd.DataFrame]:
    """
    Try to read a prep file and return per-game totals and (if possible) per-team projections.
    Output columns when successful:
      ['game_id','date','home_team','away_team','home_proj','away_proj','projected_real_run_total']
    """
    raw = _read_csv(path)
    if raw is None:
        return None
    df = _std(raw)

    # If the file already has a game-level total, prefer it
    game_total = _game_total_col(df)
    if game_total:
        keys = [c for c in ["game_id", "date", "home_team", "away_team"] if c in df.columns]
        out = df[keys + [game_total]].dropna(subset=[game_total]).drop_duplicates().copy()
        out = out.rename(columns={game_total: "projected_real_run_total"})
        # Try to also carry side projections if present
        for side in ["home_proj", "away_proj"]:
            if side in df.columns and side not in out.columns:
                # merge side columns if keyed
                candidate = df[keys + [side]].drop_duplicates()
                out = out.merge(candidate, on=keys, how="left")
        return out

    # Otherwise, attempt to derive by summing per-team rows into home/away
    derived = _derive_game_totals_from_team_rows(df)
    return derived

# ---- Main build --------------------------------------------------------------
def main() -> None:
    # Read schedule (required)
    sched_raw = _read_csv(SCHED_FILE)
    if sched_raw is None:
        raise SystemExit(f"❌ Missing or unreadable {SCHED_FILE}")
    sched = _std(sched_raw)

    # Normalize expected schedule columns
    # Map venue -> venue_name if needed
    if "venue_name" not in sched.columns and "venue" in sched.columns:
        sched["venue_name"] = sched["venue"]
    needed = ["game_id", "date", "home_team", "away_team", "venue_name"]
    missing_sched = [c for c in needed if c not in sched.columns]
    if missing_sched:
        raise SystemExit(f"❌ {SCHED_FILE} missing columns: {missing_sched}")

    base = sched[needed].drop_duplicates().copy()

    # Join game_time and pitchers from todaysgames_normalized (optional)
    today_raw = _read_csv(TODAY_FILE)
    if today_raw is not None:
        today = _std(today_raw)
        # Try to align on (date, home_team, away_team) if date exists; else on teams only.
        today_keys = [k for k in ["date", "home_team", "away_team"] if k in today.columns]
        if not today_keys:
            # try with just teams
            today_keys = [k for k in ["home_team", "away_team"] if k in today.columns]
        keep_today = [c for c in today.columns if c in {"date", "home_team", "away_team", "game_time", "pitcher_home", "pitcher_away"}]
        today = today[keep_today].drop_duplicates()
        base = base.merge(today, on=[k for k in today_keys if k in base.columns], how="left")
    else:
        base["game_time"] = pd.NA
        base["pitcher_home"] = pd.NA
        base["pitcher_away"] = pd.NA

    # Pull projections from batter and pitcher prep files (best-effort)
    gb = _extract_game_totals(BATTER_FILE)
    gp = _extract_game_totals(PITCHER_FILE)

    # Merge projections: prefer explicit home/away projections to set favorite
    proj = None
    if gb is not None and gp is not None:
        # Combine: average totals where both present; keep any available side projections
        keys = [k for k in ["game_id", "date", "home_team", "away_team"] if k in base.columns]
        proj = gb.merge(gp, on=[k for k in ["game_id", "date", "home_team", "away_team"] if k in gb.columns and k in gp.columns],
                        how="outer", suffixes=("_b", "_p"))
        # Total
        proj["projected_real_run_total"] = proj[["projected_real_run_total_b", "projected_real_run_total_p"]].mean(axis=1, skipna=True)
        # Side projections if available: prefer mean of sides when both available
        for side in ["home", "away"]:
            cols = [f"{side}_proj_b", f"{side}_proj_p"]
            present = [c for c in cols if c in proj.columns]
            if present:
                proj[f"{side}_proj"] = proj[present].mean(axis=1, skipna=True)
        keep = [k for k in ["game_id", "date", "home_team", "away_team", "home_proj", "away_proj", "projected_real_run_total"] if k in proj.columns]
        proj = proj[keep].drop_duplicates()
    else:
        proj = gb if gb is not None else gp

    # Attach projections and compute favorite
    out = base.copy()
    if proj is not None:
        keys = [k for k in ["game_id", "date", "home_team", "away_team"] if k in out.columns and k in proj.columns]
        out = out.merge(proj, on=keys, how="left")

        # Determine favorite only if we have side projections; else leave blank
        if "home_proj" in out.columns and "away_proj" in out.columns:
            out["favorite"] = np.where(out["home_proj"] > out["away_proj"], out["home_team"],
                                np.where(out["away_proj"] > out["home_proj"], out["away_team"], pd.NA))
        else:
            out["favorite"] = pd.NA
    else:
        out["projected_real_run_total"] = pd.NA
        out["favorite"] = pd.NA

    # Blank/NA fields as requested
    out["favorite_correct"] = pd.NA
    out["actual_real_run_total"] = pd.NA
    out["run_total_diff"] = pd.NA
    out["home_score"] = pd.NA
    out["away_score"] = pd.NA

    # Ensure exact column order and presence
    out = _ensure_cols(out, OUT_COLS)

    # Sort by date, game_id if present
    sort_cols = [c for c in ["date", "game_id"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    GAME_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(GAME_OUT, index=False)
    print(f"✅ Wrote {len(out):,} rows → {GAME_OUT}")

if __name__ == "__main__":
    main()
