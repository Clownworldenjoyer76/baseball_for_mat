#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build pitcher Mega-Z projection lines.

Robust source preference:
1) data/end_chain/final/startingpitchers_final.csv  (if exists and non-empty)
2) Derived fallback from today's schedule + raw pitchers:
   data/_projections/todaysgames_normalized_fixed.csv + data/Data/pitchers.csv
3) data/cleaned/pitchers_normalized_cleaned.csv
4) data/tagged/pitchers_normalized.csv

Output:
- data/_projections/pitcher_mega_z.csv
"""

from pathlib import Path
import math
import numpy as np
import pandas as pd
from scipy.stats import zscore

# Paths
P_STARTINGP = Path("data/end_chain/final/startingpitchers_final.csv")
P_TODAY     = Path("data/_projections/todaysgames_normalized_fixed.csv")
P_RAW       = Path("data/Data/pitchers.csv")
P_CLEANED   = Path("data/cleaned/pitchers_normalized_cleaned.csv")
P_TAGGED    = Path("data/tagged/pitchers_normalized.csv")
OUTPUT_FILE = Path("data/_projections/pitcher_mega_z.csv")

def ip_to_float(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    try:
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            return np.nan
        if "." in s:
            whole, frac = s.split(".", 1)
            whole = int(float(whole))
            frac = frac[:1]
            add = 0.0 if frac == "0" else (1.0/3.0 if frac == "1" else (2.0/3.0 if frac == "2" else float(f"0.{frac}")))
            return float(whole) + float(add)
        return float(s)
    except Exception:
        return np.nan

def poisson_p_ge(k, lam):
    if lam is None or lam <= 0:
        return 0.0 if k > 0 else 1.0
    k = max(0, int(k))
    s = 0.0
    term = math.exp(-lam)
    s += term
    for x in range(1, k):
        term *= lam / x
        s += term
        if term < 1e-12:
            break
    return max(0.0, min(1.0, 1.0 - s))

def pick_first(df, options):
    for c in options:
        if c in df.columns:
            return c
    return None

def _load_nonempty_csv(p: Path):
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if df.shape[0] == 0:
        return None
    df.columns = [c.strip() for c in df.columns]
    return df

def _make_today_raw_fallback():
    if not (P_TODAY.exists() and P_RAW.exists()):
        return None
    tg = pd.read_csv(P_TODAY, dtype=str)
    req = {"pitcher_home_id","pitcher_away_id"}
    if not req.issubset(tg.columns):
        return None
    ids = pd.unique(pd.concat([tg["pitcher_home_id"], tg["pitcher_away_id"]]).astype(str))
    raw = pd.read_csv(P_RAW)
    raw.columns = [c.strip() for c in raw.columns]
    if "player_id" not in raw.columns:
        return None
    raw["player_id"] = raw["player_id"].astype(str)
    df = raw[raw["player_id"].isin(ids)].copy()
    if df.empty:
        return None
    df.columns = [c.strip() for c in df.columns]
    return df, "constructed_from_todaysgames+raw"

def load_source():
    df = _load_nonempty_csv(P_STARTINGP)
    if df is not None:
        return df, str(P_STARTINGP)

    built = _make_today_raw_fallback()
    if built is not None:
        return built

    df = _load_nonempty_csv(P_CLEANED)
    if df is not None:
        return df, str(P_CLEANED)

    df = _load_nonempty_csv(P_TAGGED)
    if df is not None:
        return df, str(P_TAGGED)

    raise SystemExit("No suitable pitcher source found.")

def main():
    df, src = load_source()

    pid_col = "player_id" if "player_id" in df.columns else None
    if pid_col is None:
        raise SystemExit(f"Missing player_id in source {src}")

    name_col = "last_name, first_name" if "last_name, first_name" in df.columns else ("name" if "name" in df.columns else pid_col)
    team_col = "team" if "team" in df.columns else None

    ip_col   = pick_first(df, ["p_formatted_ip","innings_pitched","ip","ip_season"])
    k_col    = pick_first(df, ["strikeout","strikeouts","k","K"])
    bb_col   = pick_first(df, ["walk","walks","bb","BB"])
    apps_col = pick_first(df, ["apps","appearances","games_started","gs","games"])

    df[pid_col] = df[pid_col].astype(str)
    df[name_col] = df[name_col].astype(str)
    if team_col:
        df[team_col] = df[team_col].astype(str)

    df["ip_season"] = df[ip_col].apply(ip_to_float) if ip_col else np.nan
    df["strikeouts"] = pd.to_numeric(df[k_col], errors="coerce") if k_col else np.nan
    df["walks"]      = pd.to_numeric(df[bb_col], errors="coerce") if bb_col else np.nan
    df["apps"]       = pd.to_numeric(df[apps_col], errors="coerce") if apps_col else np.nan

    df["ip_per_app"] = np.where(
        (df["ip_season"].notna()) & (df["apps"].notna()) & (df["apps"] > 0),
        df["ip_season"] / df["apps"],
        np.nan
    )
    ip_per_app_median = float(df["ip_per_app"].median(skipna=True)) if df["ip_per_app"].notna().any() else 5.5
    df["ip_per_app"] = df["ip_per_app"].fillna(ip_per_app_median).clip(0.0, 9.0)

    with np.errstate(divide="ignore", invalid="ignore"):
        df["K_per_ip"]  = (df["strikeouts"] / df["ip_season"]).astype(float)
        df["BB_per_ip"] = (df["walks"] / df["ip_season"]).astype(float)

    df["K_per_ip"]  = df["K_per_ip"].replace([np.inf, -np.inf], np.nan).fillna(df["K_per_ip"].median()).fillna(1.0)
    df["BB_per_ip"] = df["BB_per_ip"].replace([np.inf, -np.inf], np.nan).fillna(df["BB_per_ip"].median()).fillna(0.35)

    df["ip_proj"] = df["ip_per_app"].clip(0.0, 9.0)
    df["K_proj"]  = (df["ip_proj"] * df["K_per_ip"]).clip(0.0, 15.0)
    df["BB_proj"] = (df["ip_proj"] * df["BB_per_ip"]).clip(0.0, 8.0)

    df["strikeouts_z"] = zscore(df["K_per_ip"].fillna(df["K_per_ip"].median()), nan_policy="omit")
    df["walks_z"]      = -zscore(df["BB_per_ip"].fillna(df["BB_per_ip"].median()), nan_policy="omit")
    df["mega_z"]       = df[["strikeouts_z","walks_z"]].mean(axis=1)

    rows = []
    for _, r in df.iterrows():
        pid = str(r[pid_col])
        nm  = str(r[name_col])
        tm  = str(r[team_col]) if team_col else ""

        k_lambda  = float(r["K_proj"])  if pd.notna(r["K_proj"])  else 0.0
        bb_lambda = float(r["BB_proj"]) if pd.notna(r["BB_proj"]) else 0.0

        for line in [4.5, 5.5, 6.5]:
            k = int(math.ceil(line))
            rows.append({
                "player_id": pid, "name": nm, "team": tm, "prop_type": "strikeouts", "line": line,
                "value": round(k_lambda, 3),
                "z_score": round(float(r["strikeouts_z"]) if pd.notna(r["strikeouts_z"]) else 0.0, 4),
                "mega_z":  round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
                "over_probability": float(max(0.02, min(0.98, poisson_p_ge(k, k_lambda)))),
            })
        for line in [1.5, 2.5]:
            k = int(math.ceil(line))
            rows.append({
                "player_id": pid, "name": nm, "team": tm, "prop_type": "walks", "line": line,
                "value": round(bb_lambda, 3),
                "z_score": round(float(r["walks_z"]) if pd.notna(r["walks_z"]) else 0.0, 4),
                "mega_z":  round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
                "over_probability": float(max(0.02, min(0.98, poisson_p_ge(k, bb_lambda)))),
            })

    out_df = pd.DataFrame(rows)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote: {OUTPUT_FILE}  (rows={len(out_df)})  source={src}")

if __name__ == "__main__":
    main()
