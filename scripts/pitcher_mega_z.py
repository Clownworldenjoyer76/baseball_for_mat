#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import zscore
import math

# Prefer cleaned; fall back to final; last resort to old tagged file
CANDIDATES = [
    Path("data/cleaned/pitchers_normalized_cleaned.csv"),
    Path("data/end_chain/final/startingpitchers_final.csv"),
    Path("data/tagged/pitchers_normalized.csv"),
]
OUTPUT_FILE = Path("data/_projections/pitcher_mega_z.csv")

def ip_to_float(v):
    try:
        s = str(v).strip()
        if s == "" or s.lower() == "nan": return np.nan
        if "." not in s: return float(s)
        whole, frac = s.split(".", 1)
        whole = int(whole); frac_i = int(frac) if frac.isdigit() else None
        if frac_i == 1: add = 1/3
        elif frac_i == 2: add = 2/3
        elif frac_i == 0: add = 0.0
        else: return float(v)
        return whole + add
    except Exception:
        try: return float(v)
        except Exception: return np.nan

def poisson_p_ge(k, lam):
    lam = float(lam) if lam is not None else 0.0
    if lam <= 0: return 0.0 if k > 0 else 1.0
    if k == 1: return 1.0 - math.exp(-lam)
    if k == 2: return 1.0 - math.exp(-lam) * (1.0 + lam)
    term = math.exp(-lam); cdf = term; n = 0
    while n < (k - 1) and term > 1e-12 and n < 200:
        n += 1; term *= lam / n; cdf += term
    return max(0.0, 1.0 - cdf)

def clamp(x, lo, hi):
    try: return max(lo, min(hi, float(x)))
    except Exception: return lo

# ----- load first existing -----
src = None
for p in CANDIDATES:
    if p.exists():
        src = p
        break
if src is None:
    raise SystemExit("❌ No pitcher source found in expected locations.")

df = pd.read_csv(src)
df.columns = [c.strip() for c in df.columns]

# Identify id/name/team
pid_col = "player_id" if "player_id" in df.columns else None
if pid_col is None:
    raise SystemExit(f"❌ Missing player_id in {src}")

name_col = "last_name, first_name" if "last_name, first_name" in df.columns else ("name" if "name" in df.columns else None)
if name_col is None:
    name_col = pid_col  # fallback to id as name

team_col = "team" if "team" in df.columns else None

# Find innings & counting stats with aliases
ip_col = next((c for c in ["p_formatted_ip","innings_pitched","ip"] if c in df.columns), None)
k_col  = next((c for c in ["strikeout","strikeouts","k","K"] if c in df.columns), None)
bb_col = next((c for c in ["walk","walks","bb","BB"] if c in df.columns), None)
apps_col = next((c for c in ["p_game","games","g","appearances"] if c in df.columns), None)

required = [pid_col, ip_col, k_col, bb_col, apps_col]
miss = [c for c in required if c is None]
if miss:
    raise SystemExit(f"❌ Missing required columns in {src}: {miss}")

# Core conversions
df["player_id"] = df[pid_col].astype(str).str.strip()
if team_col: df["team"] = df[team_col].astype(str).str.strip()
df["name"] = df[name_col].astype(str)

# innings: accept baseball-style and decimal
if ip_col == "p_formatted_ip":
    ip = df[ip_col].apply(ip_to_float)
else:
    ip = pd.to_numeric(df[ip_col], errors="coerce")

apps = pd.to_numeric(df[apps_col], errors="coerce")
K_season = pd.to_numeric(df[k_col], errors="coerce")
BB_season = pd.to_numeric(df[bb_col], errors="coerce")

df["ip_season"] = ip
df["apps"] = apps
df["ip_per_app"] = (df["ip_season"] / df["apps"]).replace([np.inf, -np.inf], np.nan).clip(0.2, 7.2)
df["ip_proj"] = df["ip_per_app"].fillna(5.5).clip(0.2, 7.2)

df["K_per_ip"]  = (K_season / df["ip_season"]).replace([np.inf, -np.inf], np.nan)
df["BB_per_ip"] = (BB_season / df["ip_season"]).replace([np.inf, -np.inf], np.nan)
df["K_per_ip"]  = df["K_per_ip"].fillna(df["K_per_ip"].median()).fillna(1.0)
df["BB_per_ip"] = df["BB_per_ip"].fillna(df["BB_per_ip"].median()).fillna(0.35)

df["K_proj"]  = (df["ip_proj"] * df["K_per_ip"]).clip(0.0, 15.0)
df["BB_proj"] = (df["ip_proj"] * df["BB_per_ip"]).clip(0.0, 8.0)

df["strikeouts_z"] = zscore(df["K_per_ip"].fillna(df["K_per_ip"].median()), nan_policy="omit")
df["walks_z"]      = -zscore(df["BB_per_ip"].fillna(df["BB_per_ip"].median()), nan_policy="omit")
df["mega_z"]       = df[["strikeouts_z","walks_z"]].mean(axis=1)

rows = []
for _, r in df.iterrows():
    pid = r["player_id"]; nm = str(r["name"]); tm = str(r["team"]) if team_col else ""
    k_lambda  = float(r["K_proj"]) if pd.notna(r["K_proj"]) else 0.0
    bb_lambda = float(r["BB_proj"]) if pd.notna(r["BB_proj"]) else 0.0

    for line in [4.5, 5.5, 6.5]:
        k = int(math.ceil(line))
        over_p = poisson_p_ge(k, k_lambda)
        rows.append({
            "player_id": pid, "name": nm, "team": tm,
            "prop_type": "strikeouts", "line": line,
            "value": round(k_lambda, 3),
            "z_score": round(float(r["strikeouts_z"]) if pd.notna(r["strikeouts_z"]) else 0.0, 4),
            "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
            "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
        })

    for line in [1.5, 2.5]:
        k = int(math.ceil(line))
        over_p = poisson_p_ge(k, bb_lambda)
        rows.append({
            "player_id": pid, "name": nm, "team": tm,
            "prop_type": "walks", "line": line,
            "value": round(bb_lambda, 3),
            "z_score": round(float(r["walks_z"]) if pd.notna(r["walks_z"]) else 0.0, 4),
            "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
            "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
        })

out_df = pd.DataFrame(rows)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
out_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}  (rows={len(out_df)})  source={src}")
