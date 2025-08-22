#!/usr/bin/env python3
"""
Append today's batter props from data/bets/prep/batter_props_final.csv
into data/bets/player_props_history.csv, mapping columns to the history schema,
choosing over_probability by market, and filling game_id from the daily schedule.

Includes verbose debug prints. Numeric columns are sanitized to avoid
'float() argument must be a string or a real number, not NAType' crashes.
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
SCHED_FILE      = Path("data/bets/mlb_sched.csv")          # needs: game_id,date,home_team,away_team
TEAMMAP_FILE    = Path("data/Data/team_name_master.csv")   # optional for normalization

HISTORY_COLUMNS = [
    "player_id","name","team","prop","line","value",
    "over_probability","date","game_id","prop_correct","prop_sort"
]

# ---------------- helpers ----------------
def _pick_col(df: pd.DataFrame, names: list[str]) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    # return NA series with correct index length
    return pd.Series([pd.NA] * len(df), index=df.index)

def _to_num(s: pd.Series) -> pd.Series:
    """Coerce to numeric floats with NaN (never pd.NA) to avoid dtype issues."""
    s = pd.to_numeric(s, errors="coerce")
    # Ensure NumPy NaN rather than pd.NA so float() casting never hits NAType
    return s.astype("float64")

def _normalize_prob(s: pd.Series) -> pd.Series:
    """Coerce to [0,1]; values in 1..100 are treated as percentages. Then clamp to [0.01, 0.99]."""
    s = _to_num(s)
    if s is None:
        return s
    s = s.copy()
    pct_mask = (s > 1.0) & (s <= 100.0)
    s.loc[pct_mask] = s.loc[pct_mask] / 100.0
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
        return pd.DataFrame()
    sched["date"] = pd.to_datetime(sched["date"], errors="coerce").dt.date
    sched = sched[sched["date"] == today].copy()
    for col in ["home_team","away_team"]:
        sched[col] = sched[col].map(_lower_strip)
    return sched

def _maybe_build_team_normalizer() -> dict[str, str]:
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
    # common variants
    alias_map["st. louis cardinals"] = "Cardinals"
    alias_map["st louis cardinals"]  = "Cardinals"
    return alias_map

def _normalize_team(team_series: pd.Series, alias_map: dict[str, str]) -> pd.Series:
    if not alias_map:
        return team_series.astype(str).map(lambda x: str(x).strip())
    return team_series.astype(str).map(lambda x: alias_map.get(_lower_strip(x), str(x).strip()))

# ---------------- main ----------------
def main() -> None:
    # ---------- load prep ----------
    if not PREP_FILE.exists():
        print(f"❌ Missing prep file: {PREP_FILE}")
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not HISTORY_FILE.exists():
            pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_FILE, index=False)
        return

    df = pd.read_csv(PREP_FILE)
    print(f"[DEBUG] Loaded prep: shape={df.shape}, columns={list(df.columns)}")
    if df.empty:
        print("[DEBUG] Prep is empty; ensure history exists with headers and exit.")
        if not HISTORY_FILE.exists():
            pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_FILE, index=False)
        return

    # normalize columns
    df.columns = [c.strip().lower() for c in df.columns]

    # ---------- build output frame ----------
    out = pd.DataFrame(index=df.index)
    # Keep player_id as nullable Int64 to avoid float coercion; safe in CSV.
    out["player_id"] = pd.to_numeric(_pick_col(df, ["player_id","id"]), errors="coerce").astype("Int64")
    out["name"]      = _pick_col(df, ["player_name","name"])
    out["team"]      = _pick_col(df, ["team","team_name","team_abbr","team_code"]).astype(str)

    out["prop"] = _pick_col(df, ["prop_type","prop","market"]).astype(str).str.strip().str.lower()

    # numeric columns via _to_num -> float64 with NaN (never pd.NA)
    out["line"]  = _to_num(_pick_col(df, ["prop_line","line"]))
    out["value"] = _to_num(_pick_col(df, ["value","odds","price"]))

    # date
    date_col = _pick_col(df, ["date","asof","timestamp","pulled_at","updated_at"])
    parsed_date = pd.to_datetime(date_col, errors="coerce").dt.date
    today = dt.date.today()
    if parsed_date.isna().all():
        parsed_date = pd.Series([today] * len(df), index=df.index)
    out["date"] = parsed_date.astype(str)

    # game id
    out["game_id"] = _pick_col(df, ["game_id"])

    # over_probability by market (vectorized) then normalized & clamped
    over_probability = pd.Series(np.nan, index=df.index, dtype="float64")
    if "over_probability" in df.columns:
        over_probability = _to_num(df["over_probability"])

    def _fill_if(colname: str, prop_name: str):
        nonlocal over_probability
        if colname in df.columns:
            mask = out["prop"].eq(prop_name)
            over_probability.loc[mask] = _to_num(df.loc[mask, colname])

    _fill_if("prob_hits_over_1p5", "hits")
    _fill_if("prob_tb_over_1p5",   "total_bases")
    _fill_if("prob_hr_over_0p5",   "home_run")
    _fill_if("prob_hr_over_0p5",   "hr")

    out["over_probability"] = _normalize_prob(over_probability)

    # placeholders / sort
    out["prop_correct"] = pd.NA
    order_map = {"hits": 10, "total_bases": 20, "home_run": 30, "hr": 30}
    out["prop_sort"] = out["prop"].map(order_map)

    # ---------- filter to today ----------
    pre_filter = len(out)
    out = out[out["date"] == str(today)].copy()
    print(f"[DEBUG] Filter to today ({today}): {len(out)} rows (from {pre_filter})")
    if out.empty:
        print("[DEBUG] No rows for today in prep after date filter; ensure history exists and exit.")
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not HISTORY_FILE.exists():
            pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_FILE, index=False)
        return

    # ---------- backfill game_id from schedule if missing ----------
    missing_mask = out["game_id"].isna() | (out["game_id"].astype(str).str.strip() == "")
    if missing_mask.any():
        sched = _load_sched_for_today(today)
        print(f"[DEBUG] Loaded schedule for today: shape={sched.shape}")
        if not sched.empty:
            alias_map = _maybe_build_team_normalizer()
            out_loc = out.loc[missing_mask, ["team"]].copy()
            out_loc["team_norm"] = _normalize_team(out_loc["team"], alias_map).map(_lower_strip)

            team_to_gid: dict[str, str] = {}
            for _, r in sched.iterrows():
                gid = r.get("game_id")
                ht  = _lower_strip(r.get("home_team"))
                at  = _lower_strip(r.get("away_team"))
                if gid and ht:
                    team_to_gid[ht] = gid
                if gid and at:
                    team_to_gid[at] = gid

            filled = out_loc["team_norm"].map(team_to_gid)
            filled_cnt = filled.notna().sum()
            out.loc[missing_mask, "game_id"] = filled.values
            print(f"[DEBUG] Backfilled game_id via schedule: {filled_cnt} rows")

    # ---------- align schema ----------
    for col in HISTORY_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[HISTORY_COLUMNS]

    # FINAL numeric sanitation (guarantee float64+NaN, not NAType)
    for col in ["line", "value", "over_probability"]:
        out[col] = _to_num(out[col])

    # ---------- union with existing, dedupe, write ----------
    if HISTORY_FILE.exists():
        hist = pd.read_csv(HISTORY_FILE)
        print(f"[DEBUG] Existing history rows: {len(hist)}")
        combined = pd.concat([hist, out], ignore_index=True)
    else:
        print("[DEBUG] History file missing; creating new.")
        combined = out

    before_dedupe = len(combined)
    combined = combined.drop_duplicates(
        subset=["player_id","prop","line","date","game_id"],
        keep="last"
    )
    print(f"[DEBUG] Combined rows before de-dup: {before_dedupe}, after de-dup: {len(combined)}")

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HISTORY_FILE, index=False)
    print(f"✅ Appended {len(out)} rows; history now {len(combined)} total rows at {HISTORY_FILE}")

if __name__ == "__main__":
    main()
