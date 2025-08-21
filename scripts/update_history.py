#!/usr/bin/env python3
"""
update_history.py — today-only writes with hard participation guards (NO lineups.csv)

Changes in this version:
- For ALL BATTER PROPS: require average ≥ 3 AB per game (season) OR projected AB/PA ≥ 3 today.
  (Fail-closed: if neither metric is available or both are below 3, drop the row.)
- Pitcher props still require projected IP ≥ 3.
- HR props still require power (season HR >= HR_MIN_SEASON_HR OR HR/PA >= HR_MIN_RATE_PER_PA).
- Probabilities clamped to [1%, 99%].

This file now has an executable main that:
- Reads current files from data/bets/
- Applies filters and overwrites those same files
"""

from __future__ import annotations
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd

# =========================
# Paths (match your repo)
# =========================
# NOTE: write to data/bets so the site reads the same files it produces.
PLAYER_HISTORY_FILE = Path("data/bets/player_props_history.csv")
GAME_HISTORY_FILE   = Path("data/bets/game_props_history.csv")

# Projection/meta sources (ONLY these — no lineups.csv)
BATTERS_META_FILE   = Path("data/Data/batters.csv")    # season totals + projections
PITCHERS_META_FILE  = Path("data/Data/pitchers.csv")   # projections for IP

# =========================
# Tunables / thresholds
# =========================
MIN_AB_FOR_BATTERS      = 3.0   # projected AB/PA floor for today
MIN_AB_AVG_PER_GAME     = 3.0   # season average AB/game floor
MIN_IP_FOR_PITCHERS     = 3.0   # projected IP floor
PROB_CLAMP_LOW          = 0.01  # 1%
PROB_CLAMP_HIGH         = 0.99  # 99%

# HR recommendation floors
HR_MIN_SEASON_HR        = 15           # require at least this many season HR
HR_MIN_RATE_PER_PA      = 0.04         # OR at least this HR/PA (~25-HR full-season pace)

# =========================
# Output schemas
# =========================
PLAYER_HISTORY_COLUMNS = [
    "player_id","name","team","prop","line","value",
    "over_probability","date","game_id","prop_correct","prop_sort"
]

GAME_HISTORY_COLUMNS = [
    "game_id","date","home_team","away_team","venue_name",
    "favorite","favorite_correct","projected_real_run_total",
    "actual_real_run_total","run_total_diff","home_score","away_score",
    "game_time","pitcher_home","pitcher_away",
    "proj_home_score","proj_away_score"
]

# =========================
# Helpers
# =========================
def _today_str() -> str:
    return str(date.today())

def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def _align(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for c in columns:
        if c not in df.columns:
            df[c] = pd.NA
    return df[columns]

def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def _clamp_probabilities(df: pd.DataFrame, col: str = "over_probability",
                         lo: float = PROB_CLAMP_LOW, hi: float = PROB_CLAMP_HIGH) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].clip(lower=lo, upper=hi)
    return df

def _is_pitcher_prop(prop: str) -> bool:
    p = str(prop or "").lower()
    return any(tok in p for tok in ["strikeout", "outs", "earned_runs", "walks_allowed", "pitches"])

def _is_hr_prop(prop: str) -> bool:
    p = str(prop or "").lower()
    return ("home_run" in p) or (p == "hr") or (" hr" in p)

# =========================
# Load projections/power
# =========================
def _load_batter_power_proj_and_avg() -> Tuple[
    Dict[str, Tuple[Optional[float], Optional[float]]],  # power: player_id -> (season_HR, HR/PA)
    Dict[str, float],                                    # proj_ab: player_id -> projected AB/PA
    Dict[str, float],                                    # avg_ab_pg: player_id -> season AB per game
]:
    """
    Returns:
      power    : player_id -> (season_HR, hr_per_pa)
      proj_ab  : player_id -> projected AB/PA (best available)
      avg_ab_pg: player_id -> season AB per game (AB / G; if G missing, uses PA with rough factor)
    """
    df = _safe_read_csv(BATTERS_META_FILE)
    if df.empty:
        return {}, {}, {}

    df.columns = [c.lower() for c in df.columns]

    # --- season HR and PA/AB for rate ---
    hr_cols = [c for c in ["hr","home_runs","season_hr","season_home_runs"] if c in df.columns]
    pa_cols = [c for c in ["pa","plate_appearances","season_pa","season_plate_appearances"] if c in df.columns]
    ab_cols = [c for c in ["ab","at_bats","season_ab","season_at_bats"] if c in df.columns]
    g_cols  = [c for c in ["g","games","season_g","season_games"] if c in df.columns]

    hr = pd.to_numeric(df[hr_cols[0]], errors="coerce") if hr_cols else pd.Series([pd.NA]*len(df))
    pa = pd.to_numeric(df[pa_cols[0]], errors="coerce") if pa_cols else None
    if pa is None and ab_cols:
        ab_for_pa = pd.to_numeric(df[ab_cols[0]], errors="coerce")
        pa = ab_for_pa * 1.13  # rough conversion if PA not available
    if pa is None:
        pa = pd.Series([pd.NA]*len(df))

    # HR/PA
    hr_pa_rate = []
    for hr_v, pa_v in zip(hr, pa):
        if pd.notna(hr_v) and pd.notna(pa_v) and float(pa_v) > 0:
            hr_pa_rate.append(float(hr_v) / float(pa_v))
        else:
            hr_pa_rate.append(None)

    # AB per GAME
    if ab_cols and g_cols:
        ab = pd.to_numeric(df[ab_cols[0]], errors="coerce")
        g  = pd.to_numeric(df[g_cols[0]], errors="coerce")
        avg_ab_pg_series = []
        for ab_v, g_v in zip(ab, g):
            if pd.notna(ab_v) and pd.notna(g_v) and float(g_v) > 0:
                avg_ab_pg_series.append(float(ab_v)/float(g_v))
            else:
                avg_ab_pg_series.append(None)
    else:
        avg_ab_pg_series = [None]*len(df)

    # Assemble dicts
    power: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    proj_ab: Dict[str, float] = {}
    avg_ab_pg: Dict[str, float] = {}

    # projected AB/PA candidates (pick first present)
    proj_candidates = {"proj_ab","projected_ab","ab","proj_pa","projected_pa","pa"}
    proj_series = None
    for cand in df.columns:
        if cand in proj_candidates:
            proj_series = pd.to_numeric(df[cand], errors="coerce")
            break

    if "player_id" in df.columns:
        for i, pid in enumerate(df["player_id"]):
            if pd.isna(pid):
                continue
            key = str(int(pid))

            season_hr = float(hr.iloc[i]) if pd.notna(hr.iloc[i]) else None
            rate      = float(hr_pa_rate[i]) if hr_pa_rate[i] is not None else None
            power[key] = (season_hr, rate)

            if proj_series is not None and pd.notna(proj_series.iloc[i]):
                proj_ab[key] = float(proj_series.iloc[i])

            aavg = avg_ab_pg_series[i]
            if aavg is not None:
                avg_ab_pg[key] = float(aavg)

    return power, proj_ab, avg_ab_pg

def _load_proj_ip() -> Dict[str, float]:
    """player_id -> projected IP (pitchers)."""
    df = _safe_read_csv(PITCHERS_META_FILE)
    if df.empty:
        return {}
    df.columns = [c.lower() for c in df.columns]
    proj_candidates = ["proj_ip","projected_ip","ip"]
    proj_ip: Dict[str, float] = {}
    for cand in proj_candidates:
        if cand in df.columns:
            series = pd.to_numeric(df[cand], errors="coerce")
            for pid, v in zip(df.get("player_id", []), series):
                if pd.notna(pid) and pd.notna(v):
                    proj_ip[str(int(pid))] = float(v)
            break
    return proj_ip

# =========================
# Row-level keep/drop logic
# =========================
def _keep_player_row(row: pd.Series,
                     proj_ab: Dict[str, float],
                     avg_ab_pg: Dict[str, float],
                     proj_ip: Dict[str, float],
                     power: Dict[str, Tuple[Optional[float], Optional[float]]]) -> bool:
    """
    Return True iff this player prop row is viable under projection/power rules.
    BATTER props: require (avg AB/game >= MIN_AB_AVG_PER_GAME) OR (projected AB/PA >= MIN_AB_FOR_BATTERS).
                  If neither metric exists or both fail → DROP (fail-closed).
    PITCHER props: require projected IP >= MIN_IP_FOR_PITCHERS (if available, else DROP).
    HR props: require power (season HR or HR/PA threshold).
    """
    pid = row.get("player_id")
    pid_key = str(int(pid)) if pd.notna(pid) else None
    prop = str(row.get("prop") or "").strip()

    # Pitcher props
    if _is_pitcher_prop(prop):
        ip = proj_ip.get(pid_key) if pid_key else None
        if ip is None or ip < MIN_IP_FOR_PITCHERS:
            return False
        return True

    # Batter props — enforce avg 3 AB rule (strict)
    avg_ab = avg_ab_pg.get(pid_key) if pid_key else None
    pab    = proj_ab.get(pid_key) if pid_key else None

    avg_ok = (avg_ab is not None and avg_ab >= MIN_AB_AVG_PER_GAME)
    proj_ok = (pab is not None and pab >= MIN_AB_FOR_BATTERS)

    if not (avg_ok or proj_ok):
        return False

    # Extra gate for HR props: power filter
    if _is_hr_prop(prop):
        season_hr, rate = power.get(pid_key, (None, None)) if pid_key else (None, None)
        season_ok = (season_hr is not None and season_hr >= HR_MIN_SEASON_HR)
        rate_ok   = (rate is not None and rate >= HR_MIN_RATE_PER_PA)
        if not (season_ok or rate_ok):
            return False

    return True

# =========================
# Write functions
# =========================
def _write_today_only(df: pd.DataFrame, path: Path, columns: List[str]) -> None:
    _ensure_parent(path)
    df = _align(df, columns)
    df = _clamp_probabilities(df, "over_probability", PROB_CLAMP_LOW, PROB_CLAMP_HIGH)

    # only today's rows
    if "date" in df.columns:
        df = df[df["date"].astype(str).str[:10] == _today_str()]

    # apply filters for player props (NO lineups)
    if path == PLAYER_HISTORY_FILE and not df.empty:
        power, proj_ab, avg_ab_pg = _load_batter_power_proj_and_avg()
        proj_ip = _load_proj_ip()
        if df.shape[0]:
            keep_mask = [
                _keep_player_row(row, proj_ab, avg_ab_pg, proj_ip, power)
                for _, row in df.iterrows()
            ]
            df = df.loc[keep_mask].copy()

    # overwrite
    if df.empty:
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        print(f"{path} -> wrote headers only (no rows after filters)")
    else:
        df.to_csv(path, index=False)
        print(f"{path} -> wrote {len(df)} row(s) for today after filters)")

def update_player_history(df: pd.DataFrame) -> None:
    _write_today_only(df, PLAYER_HISTORY_FILE, PLAYER_HISTORY_COLUMNS)

def update_game_history(df: pd.DataFrame) -> None:
    _write_today_only(df, GAME_HISTORY_FILE, GAME_HISTORY_COLUMNS)

# =========================
# Executable main
# =========================
def _main() -> int:
    player_src = _safe_read_csv(PLAYER_HISTORY_FILE)
    game_src   = _safe_read_csv(GAME_HISTORY_FILE)

    if player_src.empty and game_src.empty:
        print("No source rows found in data/bets/*history.csv — nothing to rewrite.")
        # Still create headers to ensure presence
        _write_today_only(pd.DataFrame(columns=PLAYER_HISTORY_COLUMNS),
                          PLAYER_HISTORY_FILE, PLAYER_HISTORY_COLUMNS)
        _write_today_only(pd.DataFrame(columns=GAME_HISTORY_COLUMNS),
                          GAME_HISTORY_FILE, GAME_HISTORY_COLUMNS)
        return 0

    if not player_src.empty:
        print(f"Loaded {len(player_src)} player rows; rewriting today-only with guards…")
        update_player_history(player_src)
    else:
        _write_today_only(pd.DataFrame(columns=PLAYER_HISTORY_COLUMNS),
                          PLAYER_HISTORY_FILE, PLAYER_HISTORY_COLUMNS)

    if not game_src.empty:
        print(f"Loaded {len(game_src)} game rows; rewriting today-only…")
        update_game_history(game_src)
    else:
        _write_today_only(pd.DataFrame(columns=GAME_HISTORY_COLUMNS),
                          GAME_HISTORY_FILE, GAME_HISTORY_COLUMNS)

    return 0

if __name__ == "__main__":
    raise SystemExit(_main())
