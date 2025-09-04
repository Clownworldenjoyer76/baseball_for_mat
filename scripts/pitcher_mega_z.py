#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import zscore
import math

PITCHERS_FILE = Path("data/cleaned/pitchers_normalized_cleaned.csv")
OUTPUT_FILE   = Path("data/_projections/pitcher_mega_z.csv")

def ip_to_float(ip_val):
    try:
        s = str(ip_val)
        if s.strip()=="" or s.lower()=="nan": return np.nan
        if "." not in s: return float(s)
        whole, frac = s.split(".", 1)
        whole = int(whole); frac_i = int(frac) if frac.isdigit() else 0
        add = 0.0 if frac_i==0 else (1.0/3.0 if frac_i==1 else (2.0/3.0 if frac_i==2 else float("nan")))
        return whole + (add if add==add else float(ip_val))
    except Exception:
        try: return float(ip_val)
        except Exception: return np.nan

def poisson_p_ge(k, lam):
    if lam is None or lam <= 0: return 0.0 if k>0 else 1.0
    if k==1: return 1.0 - math.exp(-lam)
    if k==2: return 1.0 - math.exp(-lam) * (1.0 + lam)
    term = math.exp(-lam); cdf = term; n = 0
    while n < (k-1) and term > 1e-12 and n < 200:
        n += 1; term *= lam / n; cdf += term
    return max(0.0, 1.0 - cdf)

def clamp(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo

df = pd.read_csv(PITCHERS_FILE)
df.columns = [c.strip() for c in df.columns]

required = ["player_id","team","strikeout","walk","p_formatted_ip","p_game"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"❌ Missing required columns in {PITCHERS_FILE}: {missing}")

name_col = "last_name, first_name" if "last_name, first_name" in df.columns else ("name" if "name" in df.columns else None)
if name_col is None:
    raise SystemExit("❌ Missing name column ('last_name, first_name' or 'name').")

df["player_id"] = df["player_id"].astype(str).str.strip()
df["team"] = df["team"].astype(str).str.strip()

df["ip_season"] = df["p_formatted_ip"].apply(ip_to_float)
df["apps"] = pd.to_numeric(df["p_game"], errors="coerce")

df["K_season"] = pd.to_numeric(df["strikeout"], errors="coerce")
df["BB_season"] = pd.to_numeric(df["walk"], errors="coerce")

df["ip_per_app"] = (df["ip_season"] / df["apps"]).replace([np.inf,-np.inf], np.nan).clip(0.2, 7.2)
df["ip_proj"] = df["ip_per_app"].fillna(5.5).clip(0.2, 7.2)

df["K_per_ip"]  = (df["K_season"]  / df["ip_season"]).replace([np.inf,-np.inf], np.nan)
df["BB_per_ip"] = (df["BB_season"] / df["ip_season"]).replace([np.inf,-np.inf], np.nan)
df["K_per_ip"]  = df["K_per_ip"].fillna(df["K_per_ip"].median()).fillna(1.0)
df["BB_per_ip"] = df["BB_per_ip"].fillna(df["BB_per_ip"].median()).fillna(0.35)

df["K_proj"]  = (df["ip_proj"] * df["K_per_ip"]).clip(0.0, 15.0)
df["BB_proj"] = (df["ip_proj"] * df["BB_per_ip"]).clip(0.0, 8.0)

df["K_per_ip_z"]  = zscore(df["K_per_ip"].fillna(df["K_per_ip"].median()), nan_policy="omit")
df["BB_per_ip_z"] = zscore(df["BB_per_ip"].fillna(df["BB_per_ip"].median()), nan_policy="omit")

df["era_z"] = 0.0
df["whip_z"] = 0.0
df["strikeouts_z"] = df["K_per_ip_z"]
df["walks_z"] = -df["BB_per_ip_z"]
df["mega_z"] = df[["strikeouts_z","walks_z"]].mean(axis=1)

rows = []
for _, r in df.iterrows():
    pid = r["player_id"]; nm = str(r[name_col]) if pd.notna(r[name_col]) else ""; tm = r["team"]
    k_lambda  = float(r["K_proj"]) if pd.notna(r["K_proj"]) else 0.0
    bb_lambda = float(r["BB_proj"]) if pd.notna(r["BB_proj"]) else 0.0

    for line in [4.5, 5.5, 6.5]:
        k = int(math.ceil(line))
        over_p = poisson_p_ge(k, k_lambda)
        rows.append({
            "player_id": pid, "name": nm, "team": tm,
            "prop_type": "strikeouts", "line": line, "value": round(k_lambda,3),
            "z_score": round(float(r["strikeouts_z"]) if pd.notna(r["strikeouts_z"]) else 0.0, 4),
            "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
            "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
        })

    for line in [1.5, 2.5]:
        k = int(math.ceil(line))
        over_p = poisson_p_ge(k, bb_lambda)
        rows.append({
            "player_id": pid, "name": nm, "team": tm,
            "prop_type": "walks", "line": line, "value": round(bb_lambda,3),
            "z_score": round(float(r["walks_z"]) if pd.notna(r["walks_z"]) else 0.0, 4),
            "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
            "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
        })

out_df = pd.DataFrame(rows)
if not out_df.empty:
    out_df["player_id"] = out_df["player_id"].astype(str)
    out_df["name"] = out_df["name"].astype(str)
    out_df["team"] = out_df["team"].astype(str)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
out_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}  (rows={len(out_df)})")
