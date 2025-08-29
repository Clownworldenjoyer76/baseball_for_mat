#!/usr/bin/env python3
# scripts/build_expanded_batter_props.py
# PURPOSE: Build per-game batter inputs using ONLY real columns from adjusted files.
# INPUTS:  data/adjusted/batters_home_adjusted.csv
#          data/adjusted/batters_away_adjusted.csv
# OUTPUT:  data/_projections/batter_props_z_expanded.csv

import pandas as pd
import numpy as np
from pathlib import Path

HOME = Path("data/adjusted/batters_home_adjusted.csv")
AWAY = Path("data/adjusted/batters_away_adjusted.csv")
OUT  = Path("data/_projections/batter_props_z_expanded.csv")

# Columns explicitly allowed/used (from your files)
ID_COLS       = ["player_id", "name", "team"]
AVG_COLS      = ["batting_avg", "avg"]                # -> proj_avg
ISO_COLS      = ["isolated_power", "xiso"]            # -> proj_iso
HR_COL        = "home_run"                            # with 'pa' -> proj_hr_rate
PA_COL        = "pa"
WEATHER_COLS  = ["weather_factor"]
CTX_COLS      = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined",
                 "venue", "location", "game_time_et", "temperature", "wind_speed", "humidity"]

def _read_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    return df

def _pick(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    return df[keep].copy()

def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def main():
    # 1) Load adjusted inputs only
    home = _read_csv(HOME)
    away = _read_csv(AWAY)
    if home.empty and away.empty:
        raise SystemExit("❌ No adjusted batter files found.")

    # 2) Keep only the allowed columns that actually exist
    allowed = list(dict.fromkeys(ID_COLS + AVG_COLS + ISO_COLS + [HR_COL, PA_COL] + WEATHER_COLS + CTX_COLS))
    home = _pick(home, allowed)
    away = _pick(away, allowed)

    df = pd.concat([home, away], ignore_index=True)

    # 3) Require player_id
    if "player_id" not in df.columns:
        raise SystemExit("❌ 'player_id' is required in adjusted inputs.")

    # 4) Collapse to one row per player_id (last seen)
    df = df.sort_index().groupby("player_id", as_index=False).last()

    # 5) Build per-game fields using ONLY real columns
    # proj_pa: 4.3 * clamp(weather_factor, 0.90–1.10); default wf=1.0 if missing
    if "weather_factor" in df.columns:
        wf = _to_num(df["weather_factor"]).fillna(1.0).clip(0.90, 1.10)
    else:
        wf = pd.Series(1.0, index=df.index)
    df["proj_pa"] = (4.3 * wf).astype(float)

    # proj_avg: prefer batting_avg, else avg; clamp 0.150–0.400; default 0.250 only if both missing
    avg_series = pd.Series(np.nan, index=df.index, dtype=float)
    if "batting_avg" in df.columns:
        avg_series = _to_num(df["batting_avg"])
    if avg_series.isna().all() and "avg" in df.columns:
        avg_series = _to_num(df["avg"])
    df["proj_avg"] = avg_series.fillna(0.250).clip(0.150, 0.400)

    # proj_iso: prefer isolated_power, else xiso; clamp 0.050–0.350; default 0.120 only if both missing
    iso_series = pd.Series(np.nan, index=df.index, dtype=float)
    if "isolated_power" in df.columns:
        iso_series = _to_num(df["isolated_power"])
    if iso_series.isna().all() and "xiso" in df.columns:
        iso_series = _to_num(df["xiso"])
    df["proj_iso"] = iso_series.fillna(0.120).clip(0.050, 0.350)

    # proj_hr_rate: (home_run / pa) when both exist; clamp 0.001–0.15; default 0.03 only if missing
    if (HR_COL in df.columns) and (PA_COL in df.columns):
        hr_pa = _to_num(df[HR_COL]) / _to_num(df[PA_COL]).replace(0, np.nan)
    else:
        hr_pa = pd.Series(np.nan, index=df.index, dtype=float)
    df["proj_hr_rate"] = hr_pa.fillna(0.03).clip(0.001, 0.15)

    # 6) Select output columns (only those present)
    out_cols = ["player_id", "proj_pa", "proj_avg", "proj_iso", "proj_hr_rate"]
    for c in (ID_COLS + WEATHER_COLS + CTX_COLS):
        if c in df.columns and c not in out_cols:
            out_cols.append(c)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df[out_cols].to_csv(OUT, index=False)
    print(f"✅ Wrote per-game expanded batter props → {OUT} (rows={len(df)})")

if __name__ == "__main__":
    main()
