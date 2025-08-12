# scripts/pitcher_mega_z.py
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import zscore
import math

# ---------- File paths ----------
PITCHERS_FILE = Path("data/tagged/pitchers_normalized.csv")   # provided normalized pitcher stats
OUTPUT_FILE   = Path("data/_projections/pitcher_mega_z.csv")  # output used by bet_tracker.py

# ---------- Helpers ----------
def ip_to_float(ip_val):
    """
    Convert baseball-style IP (e.g., 47.1, 47.2) to true innings (47.333..., 47.666...).
    If already a clean float, pass through.
    """
    try:
        s = str(ip_val)
        if s.strip() == "" or s.lower() == "nan":
            return np.nan
        if "." not in s:
            return float(s)
        whole, frac = s.split(".", 1)
        whole = int(whole)
        frac_i = int(frac) if frac.isdigit() else 0
        if frac_i == 0:
            add = 0.0
        elif frac_i == 1:
            add = 1.0/3.0
        elif frac_i == 2:
            add = 2.0/3.0
        else:
            # sometimes sources put real decimals; fallback to float
            return float(ip_val)
        return whole + add
    except Exception:
        try:
            return float(ip_val)
        except Exception:
            return np.nan

def poisson_p_ge(k, lam):
    """P(X >= k) for Poisson(lam)."""
    if lam is None or lam <= 0:
        return 0.0 if k > 0 else 1.0
    if k == 1:
        return 1.0 - math.exp(-lam)
    if k == 2:
        return 1.0 - math.exp(-lam) * (1.0 + lam)
    # generic tail via complement of CDF sum to k-1
    term = math.exp(-lam)
    cdf = term
    n = 0
    while n < (k - 1) and term > 1e-12 and n < 200:
        n += 1
        term *= lam / n
        cdf += term
    return max(0.0, 1.0 - cdf)

def clamp(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo

# ---------- Load ----------
df = pd.read_csv(PITCHERS_FILE)
df.columns = [c.strip() for c in df.columns]

required = ["player_id","team","strikeout","walk","p_formatted_ip","p_game"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"❌ Missing required columns in {PITCHERS_FILE}: {missing}")

# Name handling: prefer 'last_name, first_name' if present; else 'name'
name_col = "last_name, first_name" if "last_name, first_name" in df.columns else ("name" if "name" in df.columns else None)
if name_col is None:
    raise SystemExit("❌ Missing name column ('last_name, first_name' or 'name') in pitchers file.")

# ---------- Clean & derive base rates ----------
df["player_id"] = df["player_id"].astype(str).str.strip()
df["team"] = df["team"].astype(str).str.strip()

# innings & appearances
df["ip_season"] = df["p_formatted_ip"].apply(ip_to_float)
df["apps"] = pd.to_numeric(df["p_game"], errors="coerce")

# season K/BB totals
df["K_season"] = pd.to_numeric(df["strikeout"], errors="coerce")
df["BB_season"] = pd.to_numeric(df["walk"], errors="coerce")

# per-appearance IP (useful for starters vs relievers implicitly)
df["ip_per_app"] = (df["ip_season"] / df["apps"]).replace([np.inf, -np.inf], np.nan)
# cap to sensible bounds
df["ip_per_app"] = df["ip_per_app"].clip(lower=0.2, upper=7.2)

# projected IP this game = recent ip/appearance; fallback 5.5 if NaN
df["ip_proj"] = df["ip_per_app"].fillna(5.5).clip(lower=0.2, upper=7.2)

# K and BB per inning
df["K_per_ip"] = (df["K_season"] / df["ip_season"]).replace([np.inf, -np.inf], np.nan)
df["BB_per_ip"] = (df["BB_season"] / df["ip_season"]).replace([np.inf, -np.inf], np.nan)

# fallback sensible league-ish rates if missing
df["K_per_ip"] = df["K_per_ip"].fillna(df["K_per_ip"].median()).fillna(1.0)   # ~1 K/IP for good starters, adjust if needed
df["BB_per_ip"] = df["BB_per_ip"].fillna(df["BB_per_ip"].median()).fillna(0.35)

# Per-start projections (lambda)
df["K_proj"]  = (df["ip_proj"] * df["K_per_ip"]).clip(lower=0.0, upper=15.0)
df["BB_proj"] = (df["ip_proj"] * df["BB_per_ip"]).clip(lower=0.0, upper=8.0)

# ---------- Z-scores & mega_z (diagnostic, not used for prob directly) ----------
# Use per-inning rates for stability
df["K_per_ip_z"]  = zscore(df["K_per_ip"].fillna(df["K_per_ip"].median()), nan_policy="omit")
df["BB_per_ip_z"] = zscore(df["BB_per_ip"].fillna(df["BB_per_ip"].median()), nan_policy="omit")

# Higher K better, lower BB better
df["era_z"] = 0.0  # not using ERA here; keep column for compatibility
df["whip_z"] = 0.0
df["strikeouts_z"] = df["K_per_ip_z"]
df["walks_z"] = -df["BB_per_ip_z"]
df["mega_z"] = df[["strikeouts_z","walks_z"]].mean(axis=1)

# ---------- Build prop rows with line-aware probabilities ----------
rows = []
for _, r in df.iterrows():
    pid = r["player_id"]
    nm  = str(r[name_col]) if pd.notna(r[name_col]) else ""
    tm  = r["team"]
    k_lambda  = float(r["K_proj"]) if pd.notna(r["K_proj"]) else 0.0
    bb_lambda = float(r["BB_proj"]) if pd.notna(r["BB_proj"]) else 0.0

    # K market: lines 4.5, 5.5, 6.5
    for line in [4.5, 5.5, 6.5]:
        k = int(math.ceil(line))
        over_p = poisson_p_ge(k, k_lambda)
        rows.append({
            "player_id": pid,
            "name": nm,
            "team": tm,
            "prop_type": "strikeouts",
            "line": line,
            "value": round(k_lambda, 3),             # per-start projection (mean)
            "z_score": round(float(r["strikeouts_z"]) if pd.notna(r["strikeouts_z"]) else 0.0, 4),
            "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
            "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
        })

    # Walks market: lines 1.5, 2.5
    for line in [1.5, 2.5]:
        k = int(math.ceil(line))
        over_p = poisson_p_ge(k, bb_lambda)
        rows.append({
            "player_id": pid,
            "name": nm,
            "team": tm,
            "prop_type": "walks",
            "line": line,
            "value": round(bb_lambda, 3),            # per-start projection (mean)
            "z_score": round(float(r["walks_z"]) if pd.notna(r["walks_z"]) else 0.0, 4),
            "mega_z": round(float(r["mega_z"]) if pd.notna(r["mega_z"]) else 0.0, 4),
            "over_probability": round(clamp(over_p, 0.02, 0.98), 4),
        })

out_df = pd.DataFrame(rows)

# Clean types and order
if not out_df.empty:
    out_df["player_id"] = out_df["player_id"].astype(str)
    out_df["name"] = out_df["name"].astype(str)
    out_df["team"] = out_df["team"].astype(str)
    out_df["line"] = pd.to_numeric(out_df["line"], errors="coerce")
    out_df["value"] = pd.to_numeric(out_df["value"], errors="coerce")
    out_df["over_probability"] = pd.to_numeric(out_df["over_probability"], errors="coerce")

# Save
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
out_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote: {OUTPUT_FILE}  (rows={len(out_df)})")
