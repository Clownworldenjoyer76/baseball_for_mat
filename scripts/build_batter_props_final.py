#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
import math

BATTER_IN  = Path("data/bets/prep/batter_props_bets.csv")
OUT_FILE   = Path("data/bets/prep/batter_props_final.csv")

PROJ_CANDIDATES = [
    Path("data/_projections/batter_props_z_expanded.csv"),
    Path("data/_projections/batter_props_projected.csv"),
]

def _norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _to_scalar(x):
    if isinstance(x, pd.Series):
        x = x.dropna()
        if x.empty:
            return np.nan
        try:
            return x.iloc[0].item() if hasattr(x.iloc[0], "item") else x.iloc[0]
        except Exception:
            return x.iloc[0]
    if isinstance(x, (list, tuple, np.ndarray)):
        arr = np.asarray(x).ravel()
        return arr[0] if arr.size > 0 else np.nan
    return x

def _read_first_exists(paths):
    for p in paths:
        if p.exists():
            try:
                df = pd.read_csv(p)
                df.columns = df.columns.str.strip().str.lower()
                return df, p
            except Exception:
                continue
    return None, None

def _expected_abs(row):
    """Return scalar expected ABs for this row."""
    ab = _to_scalar(row.get("ab"))
    if pd.notna(ab):
        try:
            abf = float(ab)
            if abf > 0:
                return abf
        except Exception:
            pass
    pa = _to_scalar(row.get("pa"))
    if pd.notna(pa):
        try:
            paf = float(pa)
            if paf > 0:
                return max(1.0, 0.9 * paf)
        except Exception:
            pass
    return 4.0

def poisson_tail_over(line_val: float, lam: float) -> float:
    """
    P(X > line) for Poisson(λ) with fractional lines:
      thr = floor(line) + 1; return 1 - P(X <= thr-1)
    """
    try:
        thr = int(math.floor(float(line_val))) + 1
    except Exception:
        return np.nan
    if lam is None or not np.isfinite(lam):
        return np.nan
    lam = float(lam)
    if thr <= 0:
        return 1.0
    term = math.exp(-lam)
    acc = term
    for i in range(1, thr):
        term *= lam / i
        acc += term
        if term < 1e-15:
            break
    return float(max(0.0, min(1.0, 1.0 - acc)))

def main():
    if not BATTER_IN.exists():
        raise SystemExit(f"❌ Missing {BATTER_IN}")

    bat = pd.read_csv(BATTER_IN)
    bat.columns = bat.columns.str.strip().str.lower()

    for c in ("prop","line","value"):
        if c not in bat.columns:
            bat[c] = np.nan
    for c in ("name","team","player_id"):
        if c not in bat.columns:
            bat[c] = ""
    bat["prop"]  = bat["prop"].astype(str).str.strip().str.lower()
    bat["line"]  = pd.to_numeric(bat["line"], errors="coerce")
    bat["value"] = pd.to_numeric(bat["value"], errors="coerce")

    # Enrich with projections for ab/pa/avg/slg/totals
    proj_df, _ = _read_first_exists(PROJ_CANDIDATES)
    if proj_df is not None:
        # choose join keys
        keys = ["player_id"] if ("player_id" in bat.columns and "player_id" in proj_df.columns) else []
        if not keys:
            for k in ("name","team"):
                if k not in bat.columns: bat[k] = ""
                if k not in proj_df.columns: proj_df[k] = ""
            bat["name"] = _norm(bat["name"]); bat["team"] = _norm(bat["team"])
            proj_df["name"] = _norm(proj_df["name"]); proj_df["team"] = _norm(proj_df["team"])
            keys = [k for k in ("name","team") if k in bat.columns and k in proj_df.columns]

        keep = [c for c in (
            "player_id","name","team","ab","pa",
            "proj_hits","proj_hr","proj_avg","proj_slg",
            "b_total_bases","total_bases_projection"
        ) if c in proj_df.columns]
        proj_slim = proj_df[keep].drop_duplicates()
        bat = bat.merge(proj_slim, on=keys, how="left", suffixes=("", "_proj"))

    def lambda_from_row(r) -> float:
        prop = str(_to_scalar(r.get("prop",""))).lower()
        line = _to_scalar(r.get("line"))
        val  = _to_scalar(r.get("value"))
        ab   = _to_scalar(r.get("ab"))
        avg  = _to_scalar(r.get("proj_avg"))
        slg  = _to_scalar(r.get("proj_slg"))
        tot_hits = _to_scalar(r.get("proj_hits"))
        tot_hr   = _to_scalar(r.get("proj_hr"))
        tb_tot   = _to_scalar(r.get("b_total_bases"))
        tb_proj2 = _to_scalar(r.get("total_bases_projection"))

        exp_ab = _expected_abs(r)

        if prop == "hits":
            if pd.notna(avg) and pd.notna(exp_ab):
                p_hit = min(1.0, max(0.0, float(avg)))
                return float(p_hit * float(exp_ab))
            if pd.notna(tot_hits) and exp_ab > 0:
                # season total to per-game: assume 150 games ~ 4 AB each
                return float(max(0.0, float(tot_hits) / 150.0))
            if pd.notna(val):
                vf = float(val)
                return float(vf / 150.0) if vf > 5.0 else max(0.0, vf)
            return np.nan

        if prop == "home_runs":
            if pd.notna(tot_hr) and pd.notna(ab) and ab > 0:
                hr_rate = max(0.0, float(tot_hr) / float(ab))
                return float(hr_rate * float(exp_ab))
            if pd.notna(tot_hr):
                return float(max(0.0, float(tot_hr) / 150.0))
            if pd.notna(val):
                vf = float(val)
                return float(vf / 150.0) if vf > 2.0 else max(0.0, vf)
            return np.nan

        if prop == "total_bases":
            if pd.notna(slg) and pd.notna(exp_ab):
                return float(max(0.0, float(slg) * float(exp_ab)))
            tb_src = tb_tot if pd.notna(tb_tot) else tb_proj2
            if pd.notna(tb_src):
                return float(max(0.0, float(tb_src) / 150.0))
            if pd.notna(val):
                vf = float(val)
                return float(vf / 150.0) if vf > 8.0 else max(0.0, vf)
            return np.nan

        return max(0.0, float(val)) if pd.notna(val) else np.nan

    bat["lambda"] = bat.apply(lambda_from_row, axis=1)

    def over_prob_row(r) -> float:
        ln  = _to_scalar(r.get("line"))
        lam = _to_scalar(r.get("lambda"))
        if pd.isna(ln) or pd.isna(lam) or lam < 0:
            return np.nan
        return poisson_tail_over(float(ln), float(lam))

    bat["over_probability"] = bat.apply(over_prob_row, axis=1).clip(0, 1)

    out_cols = [
        "player_id","name","team","prop","line","value",
        "batter_z","mega_z","over_probability",
        "opp_team","opp_pitcher_name","opp_pitcher_mega_z","opp_pitcher_z"
    ]
    out_cols = [c for c in out_cols if c in bat.columns]
    out = bat[out_cols].copy() if out_cols else bat.copy()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote: {OUT_FILE} (rows={len(out)})")

if __name__ == "__main__":
    main()
