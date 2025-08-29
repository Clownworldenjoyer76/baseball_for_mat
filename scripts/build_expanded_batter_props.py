#!/usr/bin/env python3
# scripts/build_expanded_batter_props.py
# PURPOSE: Build per-game batter inputs for projections (NO downstream files read).
# INPUTS:  data/adjusted/batters_home_adjusted.csv
#          data/adjusted/batters_away_adjusted.csv
# OUTPUT:  data/_projections/batter_props_z_expanded.csv

import pandas as pd
import numpy as np
from pathlib import Path

HOME = Path("data/adjusted/batters_home_adjusted.csv")
AWAY = Path("data/adjusted/batters_away_adjusted.csv")
OUT  = Path("data/_projections/batter_props_z_expanded.csv")

def _read(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    return df

def _pick(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    return df[keep].copy()

def _num(s: pd.Series, default=np.nan) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default)

def main():
    # 1) Load adjusted inputs (only)
    home = _read(HOME)
    away = _read(AWAY)

    if home.empty and away.empty:
        raise SystemExit("❌ No adjusted batter files found.")

    # 2) Use only columns that actually exist
    base_cols = [
        "player_id", "name", "team",
        "batting_avg", "avg", "iso", "hr", "home_run", "pa",
        "weather_factor", "adj_woba_weather", "adj_woba_park", "adj_woba_combined",
        "venue", "location", "game_time_et", "temperature", "wind_speed", "humidity",
    ]
    home = _pick(home, base_cols)
    away = _pick(away, base_cols)

    df = pd.concat([home, away], ignore_index=True)

    if "player_id" not in df.columns:
        raise SystemExit("❌ 'player_id' column is required in adjusted inputs.")

    # 3) Collapse to one row per player (last seen)
    df = df.sort_index().groupby("player_id", as_index=False).last()

    # 4) Build per-game fields (bounded). Only use columns that exist.
    # Weather factor for mild scaling (default 1.0 when absent)
    wf = _num(df["weather_factor"], 1.0) if "weather_factor" in df.columns else pd.Series(1.0, index=df.index)

    # proj_pa: ~4.3 with mild weather effect (±10% cap)
    df["proj_pa"] = (4.3 * wf.clip(0.90, 1.10)).astype(float)

    # proj_avg: prefer batting_avg, else avg, else 0.250
    if "batting_avg" in df.columns:
        avg_src = _num(df["batting_avg"], np.nan)
    elif "avg" in df.columns:
        avg_src = _num(df["avg"], np.nan)
    else:
        avg_src = pd.Series(np.nan, index=df.index)
    df["proj_avg"] = avg_src.fillna(0.250).clip(0.150, 0.400)

    # proj_iso: prefer iso, else 0.120; scale with wf (±25% cap), clamp to 0.05–0.35
    if "iso" in df.columns:
        iso_src = _num(df["iso"], np.nan)
    else:
        iso_src = pd.Series(np.nan, index=df.index)
    df["proj_iso"] = (iso_src.fillna(0.120) * wf.clip(0.85, 1.25)).clip(0.050, 0.350)

    # proj_hr_rate: (hr/pa) if both present (hr OR home_run), else 0.03; scale and clamp
    hr_col = "hr" if "hr" in df.columns else ("home_run" if "home_run" in df.columns else None)
    if hr_col is not None and "pa" in df.columns:
        hr_pa = _num(df[hr_col], np.nan) / _num(df["pa"], np.nan).replace(0, np.nan)
    else:
        hr_pa = pd.Series(np.nan, index=df.index)
    df["proj_hr_rate"] = (hr_pa.fillna(0.03) * wf.clip(0.85, 1.25)).clip(0.001, 0.15)

    # 5) Select output columns (only those that exist)
    out_cols = ["player_id", "proj_pa", "proj_avg", "proj_iso", "proj_hr_rate"]
    passthru = [
        "name", "team",
        "weather_factor", "adj_woba_weather", "adj_woba_park", "adj_woba_combined",
        "venue", "location", "game_time_et", "temperature", "wind_speed", "humidity",
    ]
    out_cols += [c for c in passthru if c in df.columns]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df[out_cols].to_csv(OUT, index=False)
    print(f"✅ Wrote per-game expanded batter props → {OUT} (rows={len(df)})")

if __name__ == "__main__":
    main()
