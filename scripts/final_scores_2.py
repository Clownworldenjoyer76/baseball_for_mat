#!/usr/bin/env python3
"""
Project team runs ignoring lineups; fill data/bets/game_props_history.csv.

Method:
- Build per-PA allowed event rates from pitchers.csv (BB, 1B, 2B, 3B, HR)
- Convert events -> runs via linear weights
- Apply environment multiplier (park R * weather_factor)
- Multiply by neutral PA (away 39, home 38)
- Calibrate all-game totals to target league average (default 8.5)

Run from repo root:
    python scripts/project_team_runs_from_pitchers.py
"""

import math
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# ------------------- Config -------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
PITCHERS_CSV = REPO_ROOT / "data/Data/pitchers.csv"
PF_DAY_CSV   = REPO_ROOT / "data/Data/park_factors_day.csv"
PF_NIGHT_CSV = REPO_ROOT / "data/Data/park_factors_night.csv"
WEATHER_CSV  = REPO_ROOT / "data/weather_adjustments.csv"
HIST_CSV     = REPO_ROOT / "data/bets/game_props_history.csv"

# Day/night threshold (inclusive lower bound for "night" is >= this hour)
DAY_CUTOFF_HOUR = 18  # 6:00 PM

# Neutral plate appearances (no lineup usage)
PA_AWAY = 39.0
PA_HOME = 38.0

# Linear weights (tunable)
LW = {
    "BB": 0.33, "HBP": 0.33,
    "1B": 0.47, "2B": 0.78, "3B": 1.09, "HR": 1.40
}

# Target league average total runs per game for calibration
TARGET_AVG_TOTAL = 8.5

# Safety: cap extreme totals before calibration (helps avoid explosions)
RAW_TOTAL_CAP = 20.0


# ------------------- Helpers -------------------
def is_daytime(tstr: str) -> bool:
    """Return True if game_time is before DAY_CUTOFF_HOUR; robust to bad strings."""
    if not isinstance(tstr, str) or not tstr.strip():
        return False
    try:
        # Handles formats like "7:05 PM"
        t = pd.to_datetime(tstr.strip(), format="%I:%M %p")
        return int(t.hour) < DAY_CUTOFF_HOUR
    except Exception:
        # As a fallback, treat as night (safer wrt many parks)
        return False


def load_pitcher_rates(pitchers_csv: Path) -> pd.DataFrame:
    """From pitchers.csv, compute per-PA allowed event rates."""
    pit = pd.read_csv(pitchers_csv)
    required = ["last_name, first_name", "pa", "walk", "single", "double", "triple", "home_run"]
    missing = [c for c in required if c not in pit.columns]
    if missing:
        raise ValueError(f"Missing columns in pitchers.csv: {missing}")

    pit = pit.copy()
    denom = pit["pa"].replace(0, np.nan)
    pit["BB_rate"] = pit["walk"]     / denom
    pit["1B_rate"] = pit["single"]   / denom
    pit["2B_rate"] = pit["double"]   / denom
    pit["3B_rate"] = pit["triple"]   / denom
    pit["HR_rate"] = pit["home_run"] / denom

    # Clean
    for c in ["BB_rate","1B_rate","2B_rate","3B_rate","HR_rate"]:
        pit[c] = pit[c].fillna(0.0).clip(lower=0.0)

    # Normalize name key
    pit["name_key"] = pit["last_name, first_name"].astype(str).str.strip().str.lower()
    return pit[["name_key","BB_rate","1B_rate","2B_rate","3B_rate","HR_rate"]]


def league_avg_rates(pit_rates: pd.DataFrame) -> dict:
    return {
        "BB_rate": pit_rates["BB_rate"].mean(),
        "1B_rate": pit_rates["1B_rate"].mean(),
        "2B_rate": pit_rates["2B_rate"].mean(),
        "3B_rate": pit_rates["3B_rate"].mean(),
        "HR_rate": pit_rates["HR_rate"].mean(),
    }


def get_pitcher_rates_by_name(name: str, pit_rates: pd.DataFrame, lg: dict) -> dict:
    if isinstance(name, str) and name.strip() and name.strip().lower() != "undecided":
        key = name.strip().lower()
        m = pit_rates[pit_rates["name_key"] == key]
        if not m.empty:
            r = m.iloc[0]
            return {
                "BB_rate": float(r["BB_rate"]),
                "1B_rate": float(r["1B_rate"]),
                "2B_rate": float(r["2B_rate"]),
                "3B_rate": float(r["3B_rate"]),
                "HR_rate": float(r["HR_rate"]),
            }
    return lg


def load_park_tables(pf_day_csv: Path, pf_night_csv: Path):
    pf_day = pd.read_csv(pf_day_csv)
    pf_night = pd.read_csv(pf_night_csv)
    # Expect columns: venue, R (and others). If R missing, default to 100.
    for pf in (pf_day, pf_night):
        if "venue" not in pf.columns:
            raise ValueError("Park factor tables must include a 'venue' column.")
        if "R" not in pf.columns:
            pf["R"] = 100.0
        pf["venue_key"] = pf["venue"].astype(str).str.strip().str.lower()
    return pf_day.set_index("venue_key"), pf_night.set_index("venue_key")


def load_weather(weather_csv: Path) -> pd.DataFrame:
    # Expect columns: venue, matched_forecast_day, weather_factor
    w = pd.read_csv(weather_csv)
    for c in ["venue","matched_forecast_day","weather_factor"]:
        if c not in w.columns:
            # tolerate missing weather; return empty DF
            return pd.DataFrame(columns=["venue","matched_forecast_day","weather_factor"])
    w = w.copy()
    w["venue_key"] = w["venue"].astype(str).str.strip().str.lower()
    w["matched_forecast_day"] = pd.to_datetime(w["matched_forecast_day"], errors="coerce")
    return w[["venue_key","matched_forecast_day","weather_factor"]]


def environment_multiplier(venue_name: str, game_date: str, is_day: bool,
                           pf_day_idx: pd.DataFrame, pf_night_idx: pd.DataFrame,
                           weather_df: pd.DataFrame) -> float:
    venue_key = str(venue_name).strip().lower() if isinstance(venue_name, str) else ""
    pf_tbl = pf_day_idx if is_day else pf_night_idx

    # Park R multiplier
    R_mult = 1.0
    if venue_key in pf_tbl.index:
        val = pf_tbl.loc[venue_key, "R"]
        try:
            R_mult = float(val) / 100.0
        except Exception:
            R_mult = 1.0

    # Weather multiplier for that day (if any)
    wf = 1.0
    try:
        d = pd.to_datetime(game_date, errors="coerce")
        if pd.notna(d) and not weather_df.empty:
            cand = weather_df[(weather_df["venue_key"] == venue_key) &
                              (weather_df["matched_forecast_day"] == d)]
            if not cand.empty:
                wf_val = cand.iloc[0]["weather_factor"]
                if pd.notna(wf_val):
                    wf = float(wf_val)
    except Exception:
        pass

    return max(0.5, min(1.5, R_mult * wf))  # clamp extreme environments


def runs_per_pa_from_rates(rates: dict, env_mult: float) -> float:
    """Linear weights conversion; post-hoc environment scaling."""
    erpa = (
        rates["BB_rate"] * LW["BB"] +
        0.0               * LW["HBP"] +  # no HBP data in file; keep 0
        rates["1B_rate"] * LW["1B"] +
        rates["2B_rate"] * LW["2B"] +
        rates["3B_rate"] * LW["3B"] +
        rates["HR_rate"] * LW["HR"]
    )
    return erpa * env_mult


def main():
    # Load data
    hist = pd.read_csv(HIST_CSV)
    pit_rates = load_pitcher_rates(PITCHERS_CSV)
    lg = league_avg_rates(pit_rates)
    pf_day_idx, pf_night_idx = load_park_tables(PF_DAY_CSV, PF_NIGHT_CSV)
    weather_df = load_weather(WEATHER_CSV)

    # Ensure expected columns in history file
    needed_hist = ["date","venue_name","game_time","home_team","away_team",
                   "pitcher_home","pitcher_away"]
    miss_hist = [c for c in needed_hist if c not in hist.columns]
    if miss_hist:
        raise ValueError(f"Missing columns in game_props_history.csv: {miss_hist}")

    # Compute raw projections
    proj_home_list = []
    proj_away_list = []
    totals_list    = []
    favorite_list  = []

    for i, row in hist.iterrows():
        date = row["date"]
        venue = row["venue_name"]
        time  = row["game_time"] if "game_time" in row else ""

        is_day = is_daytime(time)
        env_mult = environment_multiplier(venue, date, is_day, pf_day_idx, pf_night_idx, weather_df)

        # Pitcher names
        ph = row.get("pitcher_home", "Undecided")
        pa = row.get("pitcher_away", "Undecided")

        # Away bats vs home pitcher
        home_pitch = get_pitcher_rates_by_name(ph, pit_rates, lg)
        erpa_away  = runs_per_pa_from_rates(home_pitch, env_mult)
        raw_away   = erpa_away * PA_AWAY

        # Home bats vs away pitcher
        away_pitch = get_pitcher_rates_by_name(pa, pit_rates, lg)
        erpa_home  = runs_per_pa_from_rates(away_pitch, env_mult)
        raw_home   = erpa_home * PA_HOME

        # Cap extreme raw totals before calibration to avoid a few games skewing scale
        raw_home = float(np.clip(raw_home, 0.0, RAW_TOTAL_CAP))
        raw_away = float(np.clip(raw_away, 0.0, RAW_TOTAL_CAP))

        proj_home_list.append(raw_home)
        proj_away_list.append(raw_away)
        totals_list.append(raw_home + raw_away)

        if raw_home > raw_away:
            favorite_list.append(row["home_team"])
        elif raw_away > raw_home:
            favorite_list.append(row["away_team"])
        else:
            favorite_list.append(np.nan)

    raw_totals = np.array(totals_list, dtype=float)
    valid_mask = np.isfinite(raw_totals) & (raw_totals > 0)

    # Calibration factor to target league average
    if valid_mask.any():
        current_avg = float(raw_totals[valid_mask].mean())
        # Guard against degenerate zero
        if current_avg <= 0:
            scale = 1.0
        else:
            scale = TARGET_AVG_TOTAL / current_avg
            # keep scaling in a reasonable range
            scale = float(np.clip(scale, 0.4, 1.6))
    else:
        scale = 1.0

    # Apply calibration
    proj_home = (np.array(proj_home_list) * scale).round(2)
    proj_away = (np.array(proj_away_list) * scale).round(2)
    totals    = (proj_home + proj_away).round(2)

    # Write back to CSV (overwrite in place)
    hist_out = hist.copy()
    hist_out["proj_home_score"] = proj_home
    hist_out["proj_away_score"] = proj_away
    hist_out["projected_real_run_total"] = totals
    hist_out["favorite"] = favorite_list

    hist_out.to_csv(HIST_CSV, index=False)
    print(f"Updated projections written to: {HIST_CSV}")
    print(f"Calibration scale applied: {scale:.3f}")
    if valid_mask.any():
        print(f"Average projected total (post-calibration): {float(totals[valid_mask].mean()):.2f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
