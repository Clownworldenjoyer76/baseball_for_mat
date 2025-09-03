#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build final batting-away (AWP) dataset.

Directives implemented:
1) Use data/end_chain/first/raw/bat_awp_dirty.csv
2) Merge player detail on player_id from data/Data/batters.csv
3) Merge team/game context on game_id from data/raw/todaysgames_normalized.csv
4) Merge weather on game_id from data/weather_adjustments.csv
5) Compute 100-based park factors using:
   - data/manual/park_factors_day.csv
   - data/manual/park_factors_night.csv
   - data/manual/park_factors_roof_closed.csv
   Logic:
     • If roof note indicates closed (notes ~ "roof closed") → roof_closed table
     • Else infer day/night from game_time_et (fallback game_time):
         - hour < 18 → day
         - hour ≥ 18 → night
     • Match by venue (preferred). If venue missing, try home_team long name.
   Outputs both:
     • park_factor_100  (computed, 100-based)
     • park_factor_src  ("day" | "night" | "roof_closed" | "unknown")
     • If a conflicting existing column named 'park_factor' is present, it is preserved as 'park_factor_raw'

6) Drop irrelevant bio fields: "last_name, first_name", year, player_age
7) Keep both game_time and game_time_et if present
8) Pitcher gaps are left as-is
9) Keep adjusted columns clearly prefixed (e.g., adj_woba_*), and do not rename raw metrics

Output:
  - data/end_chain/final/finalbatawp.csv
"""

import os
import re
import sys
import json
import subprocess
from typing import Dict, Optional

import pandas as pd


# ---------- Paths ----------
AWP_AWAY_PATH = "data/end_chain/first/raw/bat_awp_dirty.csv"
BATTERS_PATH = "data/Data/batters.csv"
GAMES_PATH = "data/raw/todaysgames_normalized.csv"
WEATHER_PATH = "data/weather_adjustments.csv"

PARK_DAY_PATH = "data/manual/park_factors_day.csv"
PARK_NIGHT_PATH = "data/manual/park_factors_night.csv"
PARK_ROOF_CLOSED_PATH = "data/manual/park_factors_roof_closed.csv"

OUTPUT_DIR = "data/end_chain/final"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "finalbatawp.csv")


# ---------- Helpers ----------
def load_csv(path: str, **kwargs) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path, **kwargs)
        # Strip whitespace off object columns
        for c in df.select_dtypes(include=["object"]).columns:
            df[c] = df[c].astype(str).str.strip()
        return df
    except FileNotFoundError:
        print(f"❌ Missing input file: {os.path.abspath(path)}")
        return None
    except Exception as e:
        print(f"❌ Error loading {path}: {e}")
        return None


def _safe_lower(s: Optional[str]) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


def parse_game_hour(row: pd.Series) -> Optional[int]:
    """
    Parse hour (0-23) from game_time_et or game_time like '6:40 PM'.
    Returns None if unparsable.
    """
    txt = row.get("game_time_et")
    if not txt or str(txt).strip() in ("NaN", "nan", "None"):
        txt = row.get("game_time")
    if not txt:
        return None
    s = str(txt).strip().upper()
    # Accept forms: '6:40 PM', '9:05AM', '18:30', '6 PM'
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)?$", s.replace(" ", ""))
    if m:
        hour = int(m.group(1))
        ampm = m.group(3)
        if ampm == "PM" and hour != 12:
            hour += 12
        if ampm == "AM" and hour == 12:
            hour = 0
        return hour
    # 24h fallback like '18:30'
    m2 = re.match(r"^(\d{1,2}):\d{2}$", s)
    if m2:
        return int(m2.group(1))
    return None


def infer_time_bucket(row: pd.Series) -> str:
    """
    Returns 'day' if hour < 18 else 'night'. If unknown, returns 'unknown'.
    """
    hour = parse_game_hour(row)
    if hour is None:
        return "unknown"
    return "day" if hour < 18 else "night"


def build_venue_factor_map(df: pd.DataFrame) -> Dict[str, float]:
    """
    Build a mapping of venue → park factor (100-based).
    If 'Park Factor' column exists, use that. Otherwise try common spellings.
    """
    df_cols = {c.lower(): c for c in df.columns}
    venue_col = df_cols.get("venue") or df_cols.get("park") or df.columns[0]
    pf_col = df_cols.get("park factor") or df_cols.get("park_factor") or df.columns[-1]
    m: Dict[str, float] = {}
    for _, r in df.iterrows():
        v = str(r.get(venue_col, "")).strip()
        if not v:
            continue
        try:
            pf = float(r.get(pf_col))
        except Exception:
            continue
        m[v.lower()] = pf
    return m


def build_team_factor_map(df: pd.DataFrame) -> Dict[str, float]:
    """
    Secondary mapping of long home_team name → park factor for backup matching.
    """
    df_cols = {c.lower(): c for c in df.columns}
    team_col = df_cols.get("home_team") or df.columns[0]
    pf_col = df_cols.get("park factor") or df_cols.get("park_factor") or df.columns[-1]
    m: Dict[str, float] = {}
    for _, r in df.iterrows():
        t = str(r.get(team_col, "")).strip()
        if not t:
            continue
        try:
            pf = float(r.get(pf_col))
        except Exception:
            continue
        m[t.lower()] = pf
    return m


def choose_park_factor(row: pd.Series,
                       day_by_venue: Dict[str, float],
                       night_by_venue: Dict[str, float],
                       roof_by_venue: Dict[str, float],
                       day_by_team: Dict[str, float],
                       night_by_team: Dict[str, float],
                       roof_by_team: Dict[str, float]) -> (Optional[float], str):
    """
    Decide which 100-based park factor to apply, and return (value, source).
    Matching priority: by venue (preferred), then by long team name.
    """
    venue = _safe_lower(row.get("venue"))
    long_team = _safe_lower(row.get("home_team"))  # weather often carries long name; if abbrev, likely won't match

    # Determine roof state via 'notes' or 'roof_type'
    notes = _safe_lower(row.get("notes"))
    roof_type = _safe_lower(row.get("roof_type"))
    roof_closed = any(k in notes for k in ["roof closed", "closed"]) or ("dome" in roof_type and "open" not in notes)

    if roof_closed:
        # roof-closed table
        pf = roof_by_venue.get(venue)
        src = "roof_closed"
        if pf is None:
            pf = roof_by_team.get(long_team)
        return (pf, src) if pf is not None else (None, "unknown")

    # Otherwise day/night
    bucket = infer_time_bucket(row)  # 'day', 'night', or 'unknown'
    if bucket == "day":
        pf = day_by_venue.get(venue)
        if pf is None:
            pf = day_by_team.get(long_team)
        return (pf, "day") if pf is not None else (None, "unknown")
    elif bucket == "night":
        pf = night_by_venue.get(venue)
        if pf is None:
            pf = night_by_team.get(long_team)
        return (pf, "night") if pf is not None else (None, "unknown")
    else:
        return (None, "unknown")


def final_bat_awp():
    print("— Building finalbatawp (dirty AWP, player_id & game_id merges) —")

    # Load core inputs
    awp = load_csv(AWP_AWAY_PATH)
    batters = load_csv(BATTERS_PATH)
    games = load_csv(GAMES_PATH)
    weather = load_csv(WEATHER_PATH)

    if any(x is None for x in (awp, batters, games, weather)):
        print("⛔ Aborting due to missing inputs.")
        return

    # Basic sanity for keys
    if "player_id" not in awp.columns:
        print("❌ AWP file missing 'player_id'.")
        return
    if "game_id" not in awp.columns:
        print("❌ AWP file missing 'game_id'.")
        return
    if "player_id" not in batters.columns:
        print("❌ batters.csv missing 'player_id'.")
        return
    if "game_id" not in games.columns:
        print("❌ todaysgames_normalized.csv missing 'game_id'.")
        return
    if "game_id" not in weather.columns:
        print("❌ weather_adjustments.csv missing 'game_id'.")
        return

    # Merge: AWP (left) + batters on player_id
    merged = pd.merge(awp, batters, on="player_id", how="left", validate="m:1")

    # Merge: + games on game_id
    merged = pd.merge(merged, games, on="game_id", how="left", suffixes=("", "_games"))

    # Merge: + weather on game_id
    # Keep any existing 'park_factor' by renaming to park_factor_raw before merge to avoid collisions
    if "park_factor" in merged.columns:
        merged = merged.rename(columns={"park_factor": "park_factor_raw"})
    weather_renamed = weather.copy()
    if "park_factor" in weather_renamed.columns:
        weather_renamed = weather_renamed.rename(columns={"park_factor": "park_factor_raw"})
    merged = pd.merge(merged, weather_renamed, on="game_id", how="left", suffixes=("", "_wx"))

    # Drop irrelevant identity fields
    for col in ['last_name, first_name', 'year', 'player_age']:
        if col in merged.columns:
            merged = merged.drop(columns=[col])

    # Load park factor manuals (100-based) and build maps
    pf_day_df = load_csv(PARK_DAY_PATH)
    pf_night_df = load_csv(PARK_NIGHT_PATH)
    pf_roof_df = load_csv(PARK_ROOF_CLOSED_PATH)

    if any(x is None for x in (pf_day_df, pf_night_df, pf_roof_df)):
        print("⚠️ Park factor files missing. Skipping computed park_factor_100.")
        merged["park_factor_100"] = pd.NA
        merged["park_factor_src"] = "unknown"
    else:
        day_by_venue = build_venue_factor_map(pf_day_df)
        night_by_venue = build_venue_factor_map(pf_night_df)
        roof_by_venue = build_venue_factor_map(pf_roof_df)

        day_by_team = build_team_factor_map(pf_day_df)
        night_by_team = build_team_factor_map(pf_night_df)
        roof_by_team = build_team_factor_map(pf_roof_df)

        # Compute per-row factor
        pf_vals = []
        pf_srcs = []
        for _, r in merged.iterrows():
            pf, src = choose_park_factor(
                r, day_by_venue, night_by_venue, roof_by_venue,
                day_by_team, night_by_team, roof_by_team
            )
            pf_vals.append(pf)
            pf_srcs.append(src)
        merged["park_factor_100"] = pf_vals
        merged["park_factor_src"] = pf_srcs

        # If there exists any old 'park_factor_raw' that looks like non-100 scaled values (e.g., < 50),
        # keep it as-is but do NOT overwrite the 100-based column.
        # (We leave redundancy for roof/notes per directive.)

    # Ensure adjusted columns remain clearly prefixed; we don't rename any raw stats.
    # (The AWP file already uses adj_woba_*; batters.csv columns are left intact.)

    # Output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)
    print(f"📝 Wrote {OUTPUT_PATH} (rows={len(merged)})")

    # Git add/commit/push
    try:
        subprocess.run(["git", "add", OUTPUT_PATH], check=True)
        subprocess.run(["git", "commit", "-m", "📊 Build finalbatawp.csv from dirty AWP; player_id & game_id merges; 100-based park factors"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("↗️ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git operation failed: {e}")


if __name__ == "__main__":
    final_bat_awp()
