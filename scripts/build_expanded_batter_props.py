#!/usr/bin/env python3
# scripts/build_expanded_batter_props.py
# PURPOSE: Merge batter projection props with existing adjusted context (home/away),
#          using only columns that actually exist in the provided CSVs. No assumptions.

import pandas as pd
import numpy as np
from pathlib import Path

# ---- Inputs (exact paths provided) ----
PROJ = Path("data/_projections/batter_props_projected.csv")
HOME = Path("data/adjusted/batters_home_adjusted.csv")
AWAY = Path("data/adjusted/batters_away_adjusted.csv")

# ---- Output ----
OUT  = Path("data/_projections/batter_props_z_expanded.csv")

def _read_csv_required(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise SystemExit(f"❌ Missing required input: {p}")
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    return df

def _pick_existing(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    return df[keep].copy()

def _to_numeric_if_present(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def main():
    # 1) Load exactly the three inputs
    proj = _read_csv_required(PROJ)
    home = _read_csv_required(HOME)
    away = _read_csv_required(AWAY)

    # 2) Validate required keys in projections (no assumptions)
    required_proj = ["player_id"]
    for c in required_proj:
        if c not in proj.columns:
            raise SystemExit(f"❌ '{c}' is required in {PROJ}")

    # 3) Build a context table from adjusted files, only using columns that exist
    context_cols = [
        # identifiers (only if present; we do NOT require them):
        "player_id", "name", "team",
        # environment / factors (confirmed present in your adjusted files):
        "weather_factor", "adj_woba_weather", "adj_woba_park", "adj_woba_combined",
        # useful context if present:
        "venue", "location", "game_time_et", "temperature", "wind_speed", "humidity",
    ]
    home_ctx = _pick_existing(home, context_cols)
    away_ctx = _pick_existing(away, context_cols)
    ctx = pd.concat([home_ctx, away_ctx], ignore_index=True)

    # 4) Collapse to one row per player_id (if duplicates exist, keep the last seen)
    if "player_id" not in ctx.columns:
        # If adjusted files lack player_id entirely, produce empty context and proceed
        ctx = pd.DataFrame(columns=["player_id"])
    ctx = ctx.sort_index().groupby("player_id", as_index=False).last()

    # 5) Clean numeric fields if present (no assumptions about units)
    numeric_maybe = [
        "weather_factor", "adj_woba_weather", "adj_woba_park", "adj_woba_combined",
        "temperature", "wind_speed", "humidity"
    ]
    ctx = _to_numeric_if_present(ctx, numeric_maybe)

    # 6) Left-merge on player_id ONLY (no date/team joins)
    merged = proj.merge(ctx, on="player_id", how="left", suffixes=("", "_ctx"))

    # 7) Defaults for missing context if any (only for factors we reference)
    if "weather_factor" not in merged.columns:
        merged["weather_factor"] = np.nan  # keep NaN to reflect absence, no guessing
    if "adj_woba_weather" not in merged.columns:
        merged["adj_woba_weather"] = np.nan
    if "adj_woba_park" not in merged.columns:
        merged["adj_woba_park"] = np.nan
    if "adj_woba_combined" not in merged.columns:
        merged["adj_woba_combined"] = np.nan

    # 8) Write output with projections + any available context columns (no renames)
    #    Keep projection columns exactly as-is.
    proj_cols_in = [c for c in proj.columns]
    ctx_cols_in  = [c for c in ["name","team","weather_factor","adj_woba_weather","adj_woba_park","adj_woba_combined",
                                "venue","location","game_time_et","temperature","wind_speed","humidity"]
                    if c in merged.columns]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged[["player_id"] + [c for c in proj_cols_in if c != "player_id"] + ctx_cols_in].to_csv(OUT, index=False)
    print(f"✅ Wrote merged file → {OUT} (rows={len(merged)})")

if __name__ == "__main__":
    main()
