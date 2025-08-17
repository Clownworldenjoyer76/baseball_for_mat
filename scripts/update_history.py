#!/usr/bin/env python3
"""
update_history.py  — write today-only history with participation & power guards (NO lineups.csv)

What it enforces:
- Today-only rows (date == today)
- Clamp over_probability into [1%, 99%]
- Participation floors from meta:
    * Batters: projected AB/PA >= MIN_AB_FOR_BATTERS
    * Pitchers: projected IP   >= MIN_IP_FOR_PITCHERS
- HR overs are dropped unless the batter shows power:
    * season HR >= HR_MIN_SEASON_HR   OR
    * HR per PA >= HR_MIN_RATE_PER_PA
"""

from __future__ import annotations
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd

# =========================
# Paths (match your repo)
# =========================
PLAYER_HISTORY_FILE = Path("data/history/player_props_history.csv")
GAME_HISTORY_FILE   = Path("data/history/game_props_history.csv")

# Projection/meta sources (ONLY these — no lineups.csv)
BATTERS_META_FILE   = Path("data/Data/batters.csv")    # should contain season HR/PA/AB and projected AB/PA
PITCHERS_META_FILE  = Path("data/Data/pitchers.csv")   # should contain projected IP

# =========================
# Tunables / thresholds
# =========================
MIN_AB_FOR_BATTERS   = 3.0    # projected AB/PA floor to keep batter props
MIN_IP_FOR_PITCHERS  = 3.0    # projected IP floor to keep pitcher props
PROB_CLAMP_LOW       = 0.01   # 1%
PROB_CLAMP_HIGH      = 0.99   # 99%

# HR recommendation floors
HR_MIN_SEASON_HR     = 15           # require at least this many season HR
HR_MIN_RATE_PER_PA   = 0.04         # OR at least this HR/PA (~25 HR full-season pace)

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
# Load projections/power (NO lineups)
# =========================
def _load_batter_power_and_proj() -> Tuple[Dict[str, Tuple[Optional[float], Optional[float]]], Dict[str, float]]:
    """
    Returns:
      power: player_id -> (season_HR, hr_per_pa)
      proj_ab: player_id -> projected AB/PA (best available)
    """
    df = _safe_read_csv(BATTERS_META_FILE)
    if df.empty:
        return {}, {}

    df.columns = [c.lower() for c in df.columns]

    # --- season HR, PA/AB for rate ---
    hr_cols = [c for c in ["hr","home_runs","season_hr","season_home_runs"] if c in df.columns]
    pa_cols = [c for c in ["pa","plate_appearances","season_pa","season_plate_appearances"] if c in df.columns]
    ab_cols = [c for c in ["ab","at_bats","season_ab","season_at_bats"] if c in df.columns]

    if hr_cols:
        hr = pd.to_numeric(df[hr_cols[0]], errors="coerce")
    else:
        hr = pd.Series([pd.NA] * len(df))

    if pa_cols:
        pa = pd.to_numeric(df[pa_cols[0]], errors="coerce")
    elif ab_cols:
        ab = pd.to_numeric(df[ab_cols[0]], errors="coerce")
        pa = ab * 1.13  # rough conversion if PA not available
    else:
        pa = pd.Series([pd.NA] * len(df))

    power: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    if "player_id" in df.columns:
        for pid, hr_v, pa_v in zip(df["player_id"], hr, pa):
            if pd.isna(pid):
                continue
            rate = float(hr_v) / float(pa_v) if (pd.notna(hr_v) and pd.notna(pa_v) and float(pa_v) > 0) else None
            power[str(int(pid))] = (float(hr_v) if pd.notna(hr_v) else None, rate)

    # --- projected AB/PA for participation floor ---
    proj_ab: Dict[str, float] = {}
    proj_candidates = ["proj_ab","projected_ab","ab","proj_pa","projected_pa","pa"]
    for cand in proj_candidates:
        if cand in df.columns:
            series = pd.to_numeric(df[cand], errors="coerce")
            for pid, v in zip(df.get("player_id", []), series):
                if pd.notna(pid) and pd.notna(v):
                    proj_ab[str(int(pid))] = float(v)
            break

    return power, proj_ab

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
                     proj_ip: Dict[str, float],
                     power: Dict[str, Tuple[Optional[float], Optional[float]]]) -> bool:
    """
    Return True iff this player prop row is viable under projection/power rules.
    (No lineup checks — by design.)
    """
    pid = row.get("player_id")
    pid_key = str(int(pid)) if pd.notna(pid) else None
    prop = str(row.get("prop") or "").strip()

    # Participation floors
    if _is_pitcher_prop(prop):
        # Pitcher props -> need projected IP
        ip = proj_ip.get(pid_key) if pid_key else None
        if ip is not None and ip < MIN_IP_FOR_PITCHERS:
            return False
    else:
        # Batter props -> need projected AB/PA
        ab = proj_ab.get(pid_key) if pid_key else None
        if ab is not None and ab < MIN_AB_FOR_BATTERS:
            return False

    # Extra: HR overs must show power
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

    # apply viability filters for player props (NO lineup usage)
    if path == PLAYER_HISTORY_FILE and not df.empty:
        power, proj_ab = _load_batter_power_and_proj()
        proj_ip        = _load_proj_ip()
        keep_mask = [
            _keep_player_row(row, proj_ab, proj_ip, power)
            for _, row in df.iterrows()
        ]
        df = df.loc[keep_mask].copy()

    # overwrite
    if df.empty:
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        print(f"{path} -> wrote headers only (no rows after filters)")
    else:
        df.to_csv(path, index=False)
        print(f"{path} -> wrote {len(df)} row(s) for today after filters")

def update_player_history(df: pd.DataFrame) -> None:
    _write_today_only(df, PLAYER_HISTORY_FILE, PLAYER_HISTORY_COLUMNS)

def update_game_history(df: pd.DataFrame) -> None:
    _write_today_only(df, GAME_HISTORY_FILE, GAME_HISTORY_COLUMNS)

if __name__ == "__main__":
    pass
