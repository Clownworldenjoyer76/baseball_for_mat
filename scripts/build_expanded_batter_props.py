#!/usr/bin/env python3
# scripts/build_expanded_batter_props.py
import pandas as pd
import numpy as np
from pathlib import Path

# Inputs
PROJ = Path("data/_projections/batter_props_projected.csv")
HOME = Path("data/adjusted/batters_home_adjusted.csv")
AWAY = Path("data/adjusted/batters_away_adjusted.csv")

# Output
OUT  = Path("data/_projections/batter_props_z_expanded.csv")

def _read_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise SystemExit(f"❌ Missing required input: {p}")
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    return df

def _as_date(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df

def _pick(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    return df[keep].copy()

def main():
    # 1) Load
    proj = _read_csv(PROJ)
    home = _read_csv(HOME)
    away = _read_csv(AWAY)

    # 2) Dates
    proj = _as_date(proj, "date")
    home = _as_date(home, "date")
    away = _as_date(away, "date")

    # 3) Build factor table from adjusted inputs (ONLY what exists; no team logic)
    factor_cols = [
        "player_id", "date", "game_id", "name", "team",
        "park_factor", "weather_factor",
        "adj_woba_combined", "adj_woba_park", "adj_woba_weather",
        "avg", "batting_avg", "iso", "hr", "home_run", "pa", "ab",
        "venue", "location", "game_time_et", "temperature", "wind_speed", "humidity"
    ]
    home_f = _pick(home, factor_cols)
    away_f = _pick(away, factor_cols)
    factors = pd.concat([home_f, away_f], ignore_index=True)

    # Prefer unified numeric types for factors
    for c in ["park_factor","weather_factor","adj_woba_combined","adj_woba_park","adj_woba_weather","iso","avg","pa","ab","hr"]:
        if c in factors.columns:
            factors[c] = pd.to_numeric(factors[c], errors="coerce")

    if "avg" not in factors.columns and "batting_avg" in factors.columns:
        factors["avg"] = pd.to_numeric(factors["batting_avg"], errors="coerce")

    # 4) Primary join on (player_id, date)
    need_cols_from_proj = ["player_id", "date"]
    for c in need_cols_from_proj:
        if c not in proj.columns:
            raise SystemExit(f"❌ Input projections missing required column: {c}")

    merged = proj.merge(factors, on=["player_id","date"], how="left", suffixes=("", "_ctx"))

    # 5) Fallback join by player_id only for rows still missing park/weather
    missing_mask = (
        (("park_factor" not in merged.columns) | merged["park_factor"].isna()) &
        (("weather_factor" not in merged.columns) | merged["weather_factor"].isna())
    )
    if missing_mask.any():
        pid_last = factors.sort_values("date").groupby("player_id", as_index=False).last()
        fallback = proj.loc[missing_mask, ["player_id"]].merge(pid_last, on="player_id", how="left", suffixes=("", "_ctx"))
        for col in fallback.columns:
            if col in ["player_id"]:  # key
                continue
            if col not in merged.columns:
                merged[col] = np.nan
            merged.loc[missing_mask, col] = merged.loc[missing_mask, col].where(
                merged.loc[missing_mask, col].notna(), fallback[col]
            )

    # 6) Defaults if still missing
    if "park_factor" not in merged.columns:
        merged["park_factor"] = 1.0
    if "weather_factor" not in merged.columns:
        merged["weather_factor"] = 1.0
    merged["park_factor"] = pd.to_numeric(merged["park_factor"], errors="coerce").fillna(1.0)
    merged["weather_factor"] = pd.to_numeric(merged["weather_factor"], errors="coerce").fillna(1.0)

    # 7) Combined factor (optional)
    merged["combined_factor"] = (merged["park_factor"] * merged["weather_factor"]).astype(float)

    # 8) Per-game fields (bounded, no team dependency)
    cf = merged["combined_factor"].clip(0.90, 1.10)  # mild effect
    merged["proj_pa"] = (4.3 * cf).astype(float)

    if "proj_avg_used" in merged.columns:
        merged["proj_avg"] = pd.to_numeric(merged["proj_avg_used"], errors="coerce").clip(0.150, 0.400).fillna(0.250)
    elif "avg" in merged.columns:
        merged["proj_avg"] = pd.to_numeric(merged["avg"], errors="coerce").clip(0.150, 0.400).fillna(0.250)
    else:
        merged["proj_avg"] = 0.250

    iso_base = (
        pd.to_numeric(merged["proj_iso_used"], errors="coerce")
        if "proj_iso_used" in merged.columns else
        (pd.to_numeric(merged["iso"], errors="coerce") if "iso" in merged.columns else pd.Series(0.120, index=merged.index))
    )
    merged["proj_iso"] = (iso_base.fillna(0.120) * merged["combined_factor"].clip(0.85, 1.25)).clip(0.050, 0.350)

    if "proj_hr_rate_pa_used" in merged.columns:
        hr_pa = pd.to_numeric(merged["proj_hr_rate_pa_used"], errors="coerce")
    elif {"hr","pa"}.issubset(merged.columns):
        hr_pa = (pd.to_numeric(merged["hr"], errors="coerce") /
                 pd.to_numeric(merged["pa"], errors="coerce").replace(0, np.nan))
    else:
        hr_pa = pd.Series(np.nan, index=merged.index, dtype=float)
    merged["proj_hr_rate"] = (hr_pa.fillna(0.03) * merged["combined_factor"].clip(0.85, 1.25)).clip(0.001, 0.15)

    # 9) Columns to write: keep all proj inputs + factors + identifiers
    keep = []
    keep += [c for c in ["player_id","date","name","team","game_id"] if c in merged.columns]
    keep += [c for c in proj.columns if c in merged.columns]  # original projection fields
    keep += ["proj_pa","proj_avg","proj_iso","proj_hr_rate","park_factor","weather_factor","combined_factor"]
    keep += [c for c in ["adj_woba_combined","adj_woba_park","adj_woba_weather","venue","location","game_time_et","temperature","wind_speed","humidity"] if c in merged.columns]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged[keep].to_csv(OUT, index=False)
    print(f"✅ Wrote expanded batter props → {OUT} (rows={len(merged)})")

if __name__ == "__main__":
    main()
