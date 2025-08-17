#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
import math

BATTER_IN  = Path("data/bets/prep/batter_props_bets.csv")
OUT_FILE   = Path("data/bets/prep/batter_props_final.csv")

# Optional projection sources to enrich with AB / rates (prefer z_expanded, else projected)
PROJ_CANDIDATES = [
    Path("data/_projections/batter_props_z_expanded.csv"),
    Path("data/_projections/batter_props_projected.csv"),
]

def _norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _to_scalar(x):
    if isinstance(x, pd.Series):
        x = x.dropna()
        return x.iloc[0] if not x.empty else np.nan
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

def _expected_abs(proj_df_row):
    # expected AB: prefer explicit 'ab' if present; else estimate from PA or fallback 4.0
    ab = proj_df_row.get("ab")
    if pd.notna(ab):
        try:
            abf = float(ab)
            if abf > 0:
                return abf
        except Exception:
            pass
    pa = proj_df_row.get("pa")
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
    # sum P(0..thr-1)
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

    # Ensure required columns
    for c in ("prop","line","value"):
        if c not in bat.columns:
            bat[c] = np.nan
    for c in ("name","team","player_id"):
        if c not in bat.columns:
            bat[c] = ""
    bat["prop"] = bat["prop"].astype(str).str.strip().str.lower()
    bat["line"] = pd.to_numeric(bat["line"], errors="coerce")
    bat["value"] = pd.to_numeric(bat["value"], errors="coerce")

    # Enrich from projections (to get ab, proj_avg, proj_slg, totals if needed)
    proj_df, src_used = _read_first_exists(PROJ_CANDIDATES)
    if proj_df is not None:
        key_cols = []
        if "player_id" in bat.columns and "player_id" in proj_df.columns:
            key_cols = ["player_id"]
        else:
            # fallback: name+team join
            for k in ("name","team"):
                if k not in bat.columns: bat[k] = ""
                if k not in proj_df.columns: proj_df[k] = ""
            proj_df["name"] = _norm(proj_df["name"])
            proj_df["team"] = _norm(proj_df["team"])
            bat["name"] = _norm(bat["name"])
            bat["team"] = _norm(bat["team"])
            key_cols = [k for k in ("player_id","name","team") if k in proj_df.columns and k in bat.columns]
            if not key_cols:
                key_cols = [c for c in ("name","team") if c in proj_df.columns and c in bat.columns]

        keep = [c for c in (
            "player_id","name","team","ab","pa",
            "proj_hits","proj_hr","proj_avg","proj_slg",
            "b_total_bases","total_bases_projection"
        ) if c in proj_df.columns]
        proj_slim = proj_df[keep].drop_duplicates()
        bat = bat.merge(proj_slim, on=[k for k in key_cols if k in proj_slim.columns], how="left", suffixes=("", "_proj"))

    # Build continuous lambda per row based on per-game expectations (no caps)
    def lambda_from_row(row) -> float:
        prop = str(_to_scalar(row.get("prop",""))).lower()
        line = _to_scalar(row.get("line"))
        val  = _to_scalar(row.get("value"))
        ab   = _to_scalar(row.get("ab"))
        pa   = _to_scalar(row.get("pa"))
        avg  = _to_scalar(row.get("proj_avg"))
        slg  = _to_scalar(row.get("proj_slg"))
        tot_hits = _to_scalar(row.get("proj_hits"))
        tot_hr   = _to_scalar(row.get("proj_hr"))
        tb_tot   = _to_scalar(row.get("b_total_bases"))
        tb_proj2 = _to_scalar(row.get("total_bases_projection"))

        exp_ab = _expected_abs(row)

        # HITS: prefer per-AB hit prob (AVG), else per-game from totals
        if prop == "hits":
            if pd.notna(avg) and pd.notna(exp_ab):
                p_hit = max(0.0, min(1.0, float(avg)))
                return float(p_hit * float(exp_ab))
            if pd.notna(tot_hits) and float(exp_ab) > 0:
                return float(max(0.0, float(tot_hits) / float(exp_ab * 37.5)))  # ~150 games * 4 AB baseline
            if pd.notna(val):
                # Treat extreme season-like values as totals; scale down
                vf = float(val)
                if vf > 5.0:
                    return float(vf / 150.0)
                return max(0.0, vf)
            return np.nan

        # HOME RUNS: per-AB HR rate times expected AB; else scale totals
        if prop == "home_runs":
            if pd.notna(tot_hr) and pd.notna(ab) and float(ab) > 0 and pd.notna(exp_ab):
                hr_rate = max(0.0, float(tot_hr) / float(ab))
                return float(hr_rate * float(exp_ab))
            if pd.notna(tot_hr):
                return float(max(0.0, float(tot_hr) / 150.0))
            if pd.notna(val):
                vf = float(val)
                if vf > 2.0:
                    return float(vf / 150.0)
                return max(0.0, vf)
            return np.nan

        # TOTAL BASES: SLG per AB * expected AB; else scale totals
        if prop == "total_bases":
            if pd.notna(slg) and pd.notna(exp_ab):
                return float(max(0.0, float(slg) * float(exp_ab)))
            tb_src = tb_tot if pd.notna(tb_tot) else tb_proj2
            if pd.notna(tb_src):
                return float(max(0.0, float(tb_src) / 150.0))
            if pd.notna(val):
                vf = float(val)
                if vf > 8.0:
                    return float(vf / 150.0)
                return max(0.0, vf)
            return np.nan

        # Unknown prop
        if pd.notna(val):
            return max(0.0, float(val))
        return np.nan

    bat["lambda"] = bat.apply(lambda_from_row, axis=1)

    def over_prob_row(row) -> float:
        ln  = _to_scalar(row.get("line"))
        lam = _to_scalar(row.get("lambda"))
        if pd.isna(ln) or pd.isna(lam) or lam < 0:
            return np.nan
        return poisson_tail_over(float(ln), float(lam))

    bat["over_probability"] = bat.apply(over_prob_row, axis=1).clip(0, 1)

    # Keep common output columns if present
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
