#!/usr/bin/env python3
"""
Append today's batter props from data/bets/prep/batter_props_final.csv
into data/bets/player_props_history.csv, mapping columns to the history schema,
choosing over_probability by market, and filling game_id from the daily schedule.
"""

from __future__ import annotations
from pathlib import Path
import datetime as dt
import pandas as pd
import numpy as np

# Inputs/outputs
PREP_FILE       = Path("data/bets/prep/batter_props_final.csv")
HISTORY_FILE    = Path("data/bets/player_props_history.csv")

# Schedule (used to backfill game_id)
SCHED_FILE      = Path("data/bets/mlb_sched.csv")                  # must include: game_id, date, home_team, away_team
TEAMMAP_FILE    = Path("data/Data/team_name_master.csv")           # optional but preferred for normalization

HISTORY_COLUMNS = [
    "player_id","name","team","prop","line","value",
    "over_probability","date","game_id","prop_correct","prop_sort"
]

# ---------------- helpers ----------------
def _pick_col(df: pd.DataFrame, names: list[str]) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([pd.NA] * len(df), index=df.index)

def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def _normalize_prob(s: pd.Series) -> pd.Series:
    """Coerce to [0,1]; values in 1..100 are treated as percentages."""
    s = _to_num(s)
    if s is None:
        return s
    # scale percent-looking values
    s = s.copy()
    pct_mask = (s > 1.0) & (s <= 100.0)
    s.loc[pct_mask] = s.loc[pct_mask] / 100.0
    # clamp
    return s.clip(lower=0.01, upper=0.99)

def _lower_strip(x) -> str:
    return str(x).strip().lower() if pd.notna(x) else ""

def _load_sched_for_today(today: dt.date) -> pd.DataFrame:
    if not SCHED_FILE.exists():
        return pd.DataFrame()
    sched = pd.read_csv(SCHED_FILE)
    if sched.empty:
        return sched
    sched.columns = [c.strip().lower() for c in sched.columns]
    need = {"game_id","date","home_team","away_team"}
    if not need.issubset(set(sched.columns)):
        return pd.DataFrame()  # insufficient schedule info
    sched["date"] = pd.to_datetime(sched["date"], errors="coerce").dt.date
    sched = sched[sched["date"] == today].copy()
    for col in ["home_team","away_team"]:
        sched[col] = sched[col].map(_lower_strip)
    return sched

def _maybe_build_team_normalizer() -> dict[str, str]:
    """
    Build a mapping of common aliases -> canonical team_name (if TEAMMAP exists).
    Fall back to identity mapping if missing.
    """
    if not TEAMMAP_FILE.exists():
        return {}
    tm = pd.read_csv(TEAMMAP_FILE)
    tm.columns = [c.strip().lower() for c in tm.columns]
    alias_map: dict[str, str] = {}
    def _add(key, team_name):
        if pd.isna(key) or pd.isna(team_name):
            return
        alias_map[_lower_strip(key)] = str(team_name).strip()
    for _, r in tm.iterrows():
        canon = str(r.get("team_name", "")).strip()
        for c in ("team_code","abbreviation","clean_team_name","team_name"):
            if c in tm.columns:
                _add(r.get(c), canon)
        _add(str(r.get("team_name","")).lower(), canon)
    # Explicit common variants
    alias_map["st. louis cardinals"] = "Cardinals"
    alias_map["st louis cardinals"]  = "Cardinals"
    return alias_map

def _normalize_team(team_series: pd.Series, alias_map: dict[str, str]) -> pd.Series:
    if not alias_map:
        return team_series.astype(str).map(lambda x: str(x).strip())
    return team_series.astype(str).map(lambda x: alias_map.get(_lower_strip(x), str(x).strip()))

# ---------------- main ----------------
def main() -> None:
    if not PREP_FILE.exists():
        print(f"❌ Missing prep file: {PREP_FILE}")
        # ensure history file exists with headers
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not HISTORY_FILE.exists():
            pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_FILE, index=False)
        return

    df = pd.read_csv(PREP_FILE)
    if df.empty:
        print("❌ Prep file is empty")
        if not HISTORY_FILE.exists():
            pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_FILE, index=False)
        return

    # Normalize columns
    df.columns = [c.strip().lower() for c in df.columns]

    # Build output frame
    out = pd.DataFrame(index=df.index)
    out["player_id"] = _to_num(_pick_col(df, ["player_id","id"])).astype("Int64")
    out["name"]      = _pick_col(df, ["player_name","name"])
    out["team"]      = _pick_col(df, ["team","team_name","team_abbr","team_code"]).astype(str)

    # Market & line
    out["prop"] = _pick_col(df, ["prop_type","prop","market"]).astype(str).str.strip().str.lower()
    out["line"] = _to_num(_pick_col(df, ["prop_line","line"]))

    # Value (price/odds)
    out["value"] = _to_num(_pick_col(df, ["value","odds","price"]))

    # Date
    date_col = _pick_col(df, ["date","asof","timestamp","pulled_at","updated_at"])
    parsed_date = pd.to_datetime(date_col, errors="coerce").dt.date
    today = dt.date.today()
    if parsed_date.isna().all():
        parsed_date = pd.Series([today] * len(df), index=df.index)
    out["date"] = parsed_date.astype(str)

    # Start with any existing game_id
    out["game_id"] = _pick_col(df, ["game_id"])

    # Choose over_probability per-market (vectorized)
    over_probability = pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")
    if "over_probability" in df.columns:
        over_probability = df["over_probability"]

    # specific market columns (if present) — only fill where prop matches
    def _fill_if(colname: str, prop_name: str):
        nonlocal over_probability
        if colname in df.columns:
            mask = out["prop"].eq(prop_name)
            over_probability.loc[mask] = df.loc[mask, colname]

    _fill_if("prob_hits_over_1p5", "hits")
    _fill_if("prob_tb_over_1p5",   "total_bases")
    _fill_if("prob_hr_over_0p5",   "home_run")
    _fill_if("prob_hr_over_0p5",   "hr")

    out["over_probability"] = _normalize_prob(over_probability)

    # Placeholders
    out["prop_correct"] = pd.NA

    # Optional sort hint
    # (ensure lower-case prop labels; map to a stable order)
    order_map = {"hits": 10, "total_bases": 20, "home_run": 30, "hr": 30}
    out["prop_sort"] = out["prop"].map(order_map)

    # Keep only today's rows
    before = len(out)
    out = out[out["date"] == str(today)].copy()
    print(f"Prep rows today: {len(out)} (from {before})")

    if out.empty:
        print("❌ No rows for today in prep after date filter; nothing to append.")
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not HISTORY_FILE.exists():
            pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_FILE, index=False)
        return

    # --------- Backfill game_id from schedule if missing ---------
    # Try to match by (date, team in {home_team, away_team}) with normalization
    missing_mask = out["game_id"].isna() | (out["game_id"].astype(str).str.strip() == "")
    if missing_mask.any():
        sched = _load_sched_for_today(today)
        if not sched.empty:
            alias_map = _maybe_build_team_normalizer()
            # normalize team names in out to the canonical form used in schedule (best effort)
            out_loc = out.loc[missing_mask, ["team"]].copy()
            out_loc["team_norm"] = _normalize_team(out_loc["team"], alias_map).map(_lower_strip)

            # Build quick lookup from team -> game_id (home/away)
            team_to_gid: dict[str, str] = {}
            for _, r in sched.iterrows():
                gid = r.get("game_id")
                ht  = _lower_strip(r.get("home_team"))
                at  = _lower_strip(r.get("away_team"))
                if gid and ht:
                    team_to_gid[ht] = gid
                if gid and at:
                    team_to_gid[at] = gid

            # Fill
            filled = out_loc["team_norm"].map(team_to_gid)
            out.loc[missing_mask, "game_id"] = filled.values

    # --------- Align schema, union, dedupe, write ---------
    for col in HISTORY_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[HISTORY_COLUMNS]

    if HISTORY_FILE.exists():
        hist = pd.read_csv(HISTORY_FILE)
        combined = pd.concat([hist, out], ignore_index=True)
    else:
        combined = out

    combined = combined.drop_duplicates(
        subset=["player_id","prop","line","date","game_id"],
        keep="last"
    )

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HISTORY_FILE, index=False)
    print(f"✅ Appended {len(out)} rows; history now {len(combined)} total rows at {HISTORY_FILE}")


if __name__ == "__main__":
    main()
