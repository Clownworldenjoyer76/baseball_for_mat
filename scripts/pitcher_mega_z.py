#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build pitcher Mega-Z projection lines.

Priority of input sources (most current first):
1) data/end_chain/final/startingpitchers_final.csv
2) data/cleaned/pitchers_normalized_cleaned.csv
3) data/tagged/pitchers_normalized.csv

Output:
- data/_projections/pitcher_mega_z.csv

Notes:
- Flexible column detection for IP, K, BB, name/team fields.
- Safe defaults and NA handling to avoid row drops.
"""

from pathlib import Path
import math
import pandas as pd
import numpy as np
from scipy.stats import zscore

# ---- source preference (UPDATED ORDER) ----
CANDIDATES = [
    Path("data/end_chain/final/startingpitchers_final.csv"),
    Path("data/cleaned/pitchers_normalized_cleaned.csv"),
    Path("data/tagged/pitchers_normalized.csv"),
]

OUTPUT_FILE = Path("data/_projections/pitcher_mega_z.csv")

def ip_to_float(v) -> float:
    """
    Convert baseball IP (e.g., 121.2) to decimal innings:
    .0 -> +0.0
    .1 -> +1/3
    .2 -> +2/3
    Accepts strings like '148.0' or '159.2' or already-float.
    """
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    try:
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            return np.nan
        if "." in s:
            whole, frac = s.split(".", 1)
            whole = int(float(whole))
            frac = frac[:1]  # only first digit matters for baseball IP
            if frac == "0":
                add = 0.0
            elif frac == "1":
                add = 1.0 / 3.0
            elif frac == "2":
                add = 2.0 / 3.0
            else:
                # Fall back: treat as decimal
                add = float("0." + frac)
            return float(whole) + float(add)
        return float(s)
    except Exception:
        return np.nan

def poisson_p_ge(k: int, lam: float) -> float:
    """
    P(X >= k) for Poisson(lam). Uses survival summation with cutoff.
    """
    if lam is None or lam <= 0:
        return 0.0 if k > 0 else 1.0
    # Use complement CDF: sum_{x=0}^{k-1} e^-lam lam^x/x!
    # For numerical stability, cap at reasonable bounds.
    k = max(0, int(k))
    s = 0.0
    term = math.exp(-lam)  # x=0
    s += term
    for x in range(1, k):
        term *= lam / x
        s += term
        if term < 1e-12:
            break
    return max(0.0, min(1.0, 1.0 - s))

def clamp(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo

def load_source() -> tuple[pd.DataFrame, Path]:
    src = None
    for p in CANDIDATES:
        if p.exists():
            src = p
            break
    if src is None:
        raise SystemExit("❌ No pitcher source found in expected locations.")
    df = pd.read_csv(src)
    df.columns = [c.strip() for c in df.columns]
    return df, src

def pick_first(df: pd.DataFrame, options: list[str]) -> str | None:
    for c in options:
        if c in df.columns:
            return c
    return None

def main():
    df, src = load_source()

    # Identify identity fields
    pid_col = "player_id" if "player_id" in df.columns else None
    if pid_col is None:
        raise SystemExit(f"❌ Missing player_id in {src}")

    name_col = None
    if "last_name, first_name" in df.columns:
        name_col = "last_name, first_name"
    elif "name" in df.columns:
        name_col = "name"
    else:
        # fallback to id for name display
        name_col = pid_col

    team_col = "team" if "team" in df.columns else None

    # IP / K / BB columns (flexible aliases)
    ip_col = pick_first(df, ["p_formatted_ip", "innings_pitched", "ip", "ip_season"])
    k_col  = pick_first(df, ["strikeout", "strikeouts", "k", "K"])
    bb_col = pick_first(df, ["walk", "walks", "bb", "BB"])

    # Apps / appearances (optional, for IP per appearance)
    apps_col = pick_first(df, ["apps", "appearances", "games_started", "gs", "games"])

    # Coerce types, fill basics
    df[pid_col] = df[pid_col].astype(str)
    if team_col:
        df[team_col] = df[team_col].astype(str)
    df[name_col] = df[name_col].astype(str)

    # Build innings pitched (season) as float where possible
    if ip_col:
        df["ip_season"] = df[ip_col].apply(ip_to_float)
    else:
        df["ip_season"] = np.nan

    # Counting stats
    if k_col and k_col in df.columns:
        df["strikeouts"] = pd.to_numeric(df[k_col], errors="coerce")
    else:
        df["strikeouts"] = np.nan

    if bb_col and bb_col in df.columns:
        df["walks"] = pd.to_numeric(df[bb_col], errors="coerce")
    else:
        df["walks"] = np.nan

    # Apps
    if apps_col and apps_col in df.columns:
        df["apps"] = pd.to_numeric(df[apps_col], errors="coerce")
    else:
        df["apps"] = np.nan

    # Derive per-appearance IP (fallbacks)
    # Prefer ip_season / apps; fallback to a reasonable default if missing.
    df["ip_per_app"] = np.where(
        (df["ip_season"].notna()) & (df["apps"].notna()) & (df["apps"] > 0),
        df["ip_season"] / df["apps"],
        np.nan
    )
    ip_per_app_median = float(df["ip_per_app"].median(skipna=True)) if df["ip_per_app"].notna().any() else 5.5
    df["ip_per_app"] = df["ip_per_app"].fillna(ip_per_app_median).clip(0.0, 9.0)

    # Derive K/IP and BB/IP
    # If season totals exist, estimate per-IP; otherwise fallback to typical league rates.
    with np.errstate(divide="ignore", invalid="ignore"):
        df["K_per_ip"]  = (df["strikeouts"] / df["ip_season"]).astype(float)
        df["BB_per_ip"] = (df["walks"] / df["ip_season"]).astype(float)

    # Fill NA with robust medians or league-ish defaults
    if not df["K_per_ip"].notna().any():
        df["K_per_ip"] = 1.0
    df["K_per_ip"]  = df["K_per_ip"].replace([np.inf, -np.inf], np.nan)
    df["BB_per_ip"] = df["BB_per_ip"].replace([np.inf, -np.inf], np.nan)

    df["K_per_ip"]  = df["K_per_ip"].fillna(df["K_per_ip"].median()).fillna(1.0)
    df["BB_per_ip"] = df["BB_per_ip"].fillna(df["BB_per_ip"].median()).fillna(0.35)

    # Project today's innings as ip_per_app (simple baseline)
    df["ip_proj"] = df["ip_per_app"].clip(0.0, 9.0)

    # Project counting stats
    df["K_proj"]  = (df["ip_proj"] * df["K_per_ip"]).clip(0.0, 15.0)
    df["BB_proj"] = (df["ip_proj"] * df["BB_per_ip"]).clip(0.0, 8.0)

    # Z-scores and Mega-Z
    df["strikeouts_z"] = zscore(df["K_per_ip"].fillna(df["K_per_ip"].median()), nan_policy="omit")
    df["walks_z"]      = -zscore(df["BB_per_ip"].fillna(df["BB_per_ip"].median()), nan_policy="omit")
    df["mega_z"]       = df[["strikeouts_z", "walks_z"]].mean(axis=1)

    # Build output rows per pitcher for selected prop lines
    rows = []
    for _, r in df.iterrows():
        pid = str(r[pid_col])
        nm  = str(r[name_col])
        tm  = str(r[team_col]) if team_col else ""

        k_lambda  = float(r["K_proj"]) if pd.notna(r["K_proj"]) else 0.0
        bb_lambda = float(r["BB_proj"]) if pd.notna(r["BB_proj"]) else 0.0

        # Strikeouts lines
        for line in [4.5, 5.5, 6.5]:
            k = int(math.ceil(line))
            over_p = poisson_p_ge(k, k_lambda)
            rows.append({
                "player_id": pid,
                "name": nm,
                "team": tm,
                "prop_type": "strikeouts",
                "line": line,
                "value": round(k_lambda, 3),
                "z_score": round(float(r["strikeouts_z"]) if pd.notna(r["strikeouts_z"]) else 0.0, 4),
                "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
                "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
            })

        # Walks lines
        for line in [1.5, 2.5]:
            k = int(math.ceil(line))
            over_p = poisson_p_ge(k, bb_lambda)
            rows.append({
                "player_id": pid,
                "name": nm,
                "team": tm,
                "prop_type": "walks",
                "line": line,
                "value": round(bb_lambda, 3),
                "z_score": round(float(r["walks_z"]) if pd.notna(r["walks_z"]) else 0.0, 4),
                "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
                "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
            })

    out_df = pd.DataFrame(rows)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote: {OUTPUT_FILE}  (rows={len(out_df)})  source={src}")

if __name__ == "__main__":
    main()
