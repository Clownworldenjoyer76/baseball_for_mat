#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

# Inputs
BATS_PROJ = Path("data/_projections/batter_props_projected.csv")
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

def norm_team(x: pd.Series) -> pd.Series:
    s = x.astype(str).str.strip()
    key = s.str.lower().str.replace(" ", "").str.replace("-", "").str.replace("_", "")
    return s.where(~key.isin(TEAM_FIX), key.map(TEAM_FIX))

def read_csv_or_fail(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise SystemExit(f"❌ Missing required input: {p}")
    return pd.read_csv(p)

def extract_team_factors(df: pd.DataFrame, team_col: str) -> pd.DataFrame:
    cols = [c for c in [
        "player_id","name","team",team_col,"date","game_id",
        "park_factor","weather_factor","adj_woba_combined","adj_woba_park","adj_woba_weather",
        "batting_avg","avg","iso","home_run","hr","pa","ab"
    ] if c in df.columns]
    z = df[cols].copy()
    # Prefer explicit 'team' but fallback to provided team_col
    if "team" not in z.columns and team_col in z.columns:
        z["team"] = z[team_col]
    z["team"] = norm_team(z["team"])
    # Ensure numeric
    for c in ["park_factor","weather_factor","adj_woba_combined","iso","batting_avg","avg","home_run","hr","pa","ab"]:
        if c in z.columns:
            z[c] = pd.to_numeric(z[c], errors="coerce")
    # Derive safe rates
    if "avg" not in z.columns and "batting_avg" in z.columns:
        z["avg"] = z["batting_avg"]
    if "hr" not in z.columns and "home_run" in z.columns:
        z["hr"] = z["home_run"]
    return z

def main():
    # Load projected props (used for IDs/date/game keys), plus adjusted context
    bats = read_csv_or_fail(BATS_PROJ)
    home = read_csv_or_fail(HOME_ADJ)
    away = read_csv_or_fail(AWAY_ADJ)

    # Basic column hygiene
    bats.columns = [c.strip() for c in bats.columns]
    home.columns = [c.strip() for c in home.columns]
    away.columns = [c.strip() for c in away.columns]

    # Normalize team for bats
    if "team" not in bats.columns:
        raise SystemExit("❌ Expected 'team' column in batter_props_projected.csv")
    bats["team"] = norm_team(bats["team"])

    # Build factor table from home + away adjusted files
    home_f = extract_team_factors(home, "home_team" if "home_team" in home.columns else "team")
    away_f = extract_team_factors(away, "away_team" if "away_team" in away.columns else "team")
    factors = pd.concat([home_f, away_f], ignore_index=True)

    # Prefer row-level player_id match; fallback to team-level last observation
    by_pid = ["player_id"]
    have_pid = "player_id" in bats.columns and "player_id" in factors.columns
    if have_pid:
        merged = bats.merge(factors, on="player_id", how="left", suffixes=("", "_ctx"))
        # backfill team/date/game_id if missing
        for c in ["team","date","game_id"]:
            if c not in merged.columns and c+"_ctx" in merged.columns:
                merged[c] = merged[c+"_ctx"]
    else:
        # team join
        merged = bats.merge(factors.groupby("team", as_index=False).last(), on="team", how="left", suffixes=("", "_ctx"))

    # --- PER-GAME NORMALIZATION (critical) ---
    # 1) proj_pa: per-game PA default ~4.3, optionally nudge by park/weather (mild)
    base_pa = 4.3
    pf = merged.get("park_factor", pd.Series(base_pa, index=merged.index).mul(0))
    wf = merged.get("weather_factor", pd.Series(base_pa, index=merged.index).mul(0))
    # Small adjustments: +/- 5% combined max
    adj_mult = (merged.get("park_factor", 1.0) * merged.get("weather_factor", 1.0)).astype(float)
    adj_mult = adj_mult.clip(lower=0.90, upper=1.10)
    merged["proj_pa"] = base_pa * adj_mult

    # 2) proj_avg / proj_ba: use batting average, lightly capped
    if "avg" in merged.columns:
        merged["proj_avg"] = merged["avg"].clip(lower=0.150, upper=0.400)
    else:
        merged["proj_avg"] = 0.250

    # 3) proj_iso: scale iso by combined factor (capped)
    iso_base = merged.get("iso", pd.Series(0.120, index=merged.index)).astype(float)
    cf = (merged.get("park_factor", 1.0) * merged.get("weather_factor", 1.0)).astype(float).clip(0.85, 1.25)
    merged["proj_iso"] = (iso_base * cf).clip(lower=0.050, upper=0.350)

    # 4) proj_hr_rate (per PA): derive from season HR/PA, then scale by factors (capped)
    if "hr" in merged.columns and "pa" in merged.columns:
        hr_pa = (merged["hr"].astype(float) / merged["pa"].replace(0, np.nan).astype(float)).clip(lower=0.0)
        hr_pa = hr_pa.fillna(0.0)
    else:
        hr_pa = pd.Series(0.03, index=merged.index, dtype=float)  # ~3% per PA baseline
    merged["proj_hr_rate"] = (hr_pa * cf).clip(lower=0.001, upper=0.15)

    # Keep key identifiers if present
    keep_ids = [c for c in ["player_id","name","team","date","game_id"] if c in merged.columns]
    out = merged[keep_ids + [c for c in merged.columns if c.startswith("proj_")] + [
        c for c in ["park_factor","weather_factor","adj_woba_combined","adj_woba_park","adj_woba_weather"] if c in merged.columns
    ]].copy()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"✅ Wrote expanded batter props → {OUT} (rows={len(out)})")

if __name__ == "__main__":
    main()
