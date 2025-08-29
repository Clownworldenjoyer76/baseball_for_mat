#!/usr/bin/env python3
# scripts/build_expanded_batter_props.py
import pandas as pd
import numpy as np
from pathlib import Path

# Inputs
BATS_PROJ = Path("data/_projections/batter_props_projected.csv")  # columns provided by user
HOME_ADJ  = Path("data/adjusted/batters_home_adjusted.csv")
AWAY_ADJ  = Path("data/adjusted/batters_away_adjusted.csv")

# Output
OUT = Path("data/_projections/batter_props_z_expanded.csv")

TEAM_FIX = {
    "redsox": "Red Sox",
    "whitesox": "White Sox",
    "bluejays": "Blue Jays",
    "dbacks": "D-backs",
    "diamondbacks": "D-backs",
}

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def _norm_team(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    key = s.str.lower().str.replace(" ", "").str.replace("-", "").str.replace("_", "")
    return s.where(~key.isin(TEAM_FIX), key.map(TEAM_FIX))

def _read_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise SystemExit(f"❌ Missing required input: {p}")
    return _norm_cols(pd.read_csv(p))

def _coerce_num(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _safe_date(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df

def _extract_ctx(df: pd.DataFrame, team_col_hint: str) -> pd.DataFrame:
    """Return a lightweight context table with player_id/team/date/game_id & factors if present."""
    cols_keep = []
    for c in [
        "player_id","name","team",team_col_hint,"date","game_id",
        "park_factor","weather_factor","adj_woba_combined","adj_woba_park","adj_woba_weather",
        "avg","batting_avg","iso","hr","home_run","pa","ab"
    ]:
        if c in df.columns: cols_keep.append(c)
    ctx = df[cols_keep].copy()

    # Normalize team column (prefer 'team', fallback to hint)
    if "team" not in ctx.columns and team_col_hint in ctx.columns:
        ctx["team"] = ctx[team_col_hint]
    if "team" in ctx.columns:
        ctx["team"] = _norm_team(ctx["team"])

    # Normalize numeric fields
    ctx = _coerce_num(ctx, ["park_factor","weather_factor","adj_woba_combined","iso","avg","batting_avg","hr","home_run","pa","ab"])
    if "avg" not in ctx.columns and "batting_avg" in ctx.columns:
        ctx["avg"] = ctx["batting_avg"]
    if "hr" not in ctx.columns and "home_run" in ctx.columns:
        ctx["hr"] = ctx["home_run"]

    # Normalize date if present
    ctx = _safe_date(ctx, "date")
    return ctx

def main():
    # Load inputs
    bats = _read_csv(BATS_PROJ)
    home = _read_csv(HOME_ADJ)
    away = _read_csv(AWAY_ADJ)

    # Normalize dates
    bats = _safe_date(bats, "date")
    home = _safe_date(home, "date")
    away = _safe_date(away, "date")

    # Build context from home/away adjusted
    home_ctx = _extract_ctx(home, "home_team" if "home_team" in home.columns else "team")
    away_ctx = _extract_ctx(away, "away_team" if "away_team" in away.columns else "team")
    factors = pd.concat([home_ctx, away_ctx], ignore_index=True)

    # Prefer player_id join; if that fails, later we can fallback to team-level averages
    have_pid = "player_id" in bats.columns and "player_id" in factors.columns

    # Prepare base output starting from bats
    base_cols = [c for c in ["player_id","date"] if c in bats.columns]
    out = bats[base_cols].copy()

    # Attach context by player_id (and same date if both have date)
    merged = bats.copy()
    if have_pid:
        if "date" in bats.columns and "date" in factors.columns:
            merged = merged.merge(factors, on=["player_id","date"], how="left", suffixes=("", "_ctx"))
            # If no date match, try player_id only (backfill)
            need = merged["player_id"].notna() & (
                (("park_factor" in merged.columns) & merged["park_factor"].isna()) |
                (("weather_factor" in merged.columns) & merged["weather_factor"].isna())
            )
            if need.any():
                pid_only = bats.loc[need, ["player_id"]].merge(
                    factors.groupby("player_id", as_index=False).last(), on="player_id", how="left"
                )
                merged.loc[need, pid_only.columns] = pid_only.values
        else:
            merged = merged.merge(factors.groupby("player_id", as_index=False).last(), on="player_id", how="left", suffixes=("", "_ctx"))
    else:
        # If no player_id in ctx, leave factors empty (we will default below)
        pass

    # Backfill identifiers from context when missing
    for idc in ["team","game_id"]:
        if idc not in merged.columns and f"{idc}_ctx" in merged.columns:
            merged[idc] = merged[f"{idc}_ctx"]

    # Factors default to 1.0 if missing
    for fc in ["park_factor","weather_factor"]:
        if fc not in merged.columns:
            merged[fc] = 1.0
        merged[fc] = pd.to_numeric(merged[fc], errors="coerce").fillna(1.0)

    # Combined factor (optional)
    merged["combined_factor"] = (merged["park_factor"] * merged["weather_factor"]).astype(float)
    cf = merged["combined_factor"].clip(lower=0.85, upper=1.25)

    # ---------- PER-GAME FIELDS ----------
    # proj_pa: base ~4.3, mildly nudged by factors (±10% cap)
    merged["proj_pa"] = (4.3 * cf.clip(0.90, 1.10)).astype(float)

    # proj_avg: prefer context avg; else fallback 0.250
    if "avg" in merged.columns:
        merged["proj_avg"] = pd.to_numeric(merged["avg"], errors="coerce").clip(0.150, 0.400).fillna(0.250)
    else:
        merged["proj_avg"] = 0.250

    # proj_iso: prefer context iso; else fallback 0.120; scale by cf cap (0.05–0.35)
    iso_base = pd.to_numeric(merged["iso"], errors="coerce") if "iso" in merged.columns else pd.Series(0.120, index=merged.index)
    merged["proj_iso"] = (iso_base.fillna(0.120) * cf).clip(0.050, 0.350)

    # proj_hr_rate (per PA): from context hr/pa if present; else from bats 'proj_hr_rate_pa_used'; else 0.03
    if "hr" in merged.columns and "pa" in merged.columns:
        hr_pa = (pd.to_numeric(merged["hr"], errors="coerce") /
                 pd.to_numeric(merged["pa"], errors="coerce").replace(0, np.nan)).clip(lower=0.0)
        hr_pa = hr_pa.fillna(np.nan)
    else:
        hr_pa = pd.Series(np.nan, index=merged.index, dtype=float)

    if hr_pa.isna().all():
        if "proj_hr_rate_pa_used" in merged.columns:
            hr_pa = pd.to_numeric(merged["proj_hr_rate_pa_used"], errors="coerce")
        else:
            hr_pa = pd.Series(0.03, index=merged.index, dtype=float)

    merged["proj_hr_rate"] = (hr_pa.fillna(0.03) * cf).clip(0.001, 0.15)

    # ---------- OUTPUT ----------
    keep_ids = [c for c in ["player_id","date","name","team","game_id"] if c in merged.columns]
    keep_proj = ["proj_pa","proj_avg","proj_iso","proj_hr_rate"]
    keep_factors = [c for c in ["park_factor","weather_factor","combined_factor","adj_woba_combined","adj_woba_park","adj_woba_weather"] if c in merged.columns]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged[keep_ids + keep_proj + keep_factors].to_csv(OUT, index=False)
    print(f"✅ Wrote expanded batter props → {OUT} (rows={len(merged)})")

if __name__ == "__main__":
    main()
