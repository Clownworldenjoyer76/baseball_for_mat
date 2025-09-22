#!/usr/bin/env python3
from __future__ import annotations
import math
from pathlib import Path
from typing import Optional, Iterable, Tuple
import numpy as np
import pandas as pd

# ---------- Inputs ----------
IN_CANDIDATES: Iterable[Path] = [
    Path("data/_projections/batter_props_z_expanded.csv"),
    Path("data/batter_props_z_expanded.csv"),
    Path("batter_props_z_expanded.csv"),
]
BATTERS_CANDIDATES: Iterable[Path] = [
    Path("data/Data/batters.csv"),
    Path("data/data/batters.csv"),
    Path("data/batters.csv"),
    Path("batters.csv"),
]
LINEUPS_CSV = Path("data/raw/lineups.csv")
TGN_CSV     = Path("data/raw/todaysgames_normalized.csv")

# ---------- Outputs ----------
OUT_DIR = Path("data/_projections")
OUT_PROJ_FINAL = OUT_DIR / "batter_props_projected_final.csv"
OUT_EXP_FINAL  = OUT_DIR / "batter_props_expanded_final.csv"  # minimal for adj_woba merge

SUM_DIR = Path("summaries/07_final")

def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df

def _read_first_existing(paths: Iterable[Path]) -> Optional[pd.DataFrame]:
    for p in paths:
        if p.exists():
            try:
                return _std(pd.read_csv(p))
            except Exception:
                pass
    return None

def _read_csv_force_str(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().replace({"None":"","nan":"","NaN":""})
    return df

def _as_rate(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.replace("%","",regex=False)
    s = pd.to_numeric(s, errors="coerce")
    s = s.where(s<=1.0, s/100.0)
    return s.clip(lower=0.0)

def _coalesce(df: pd.DataFrame, *cols: str, default=np.nan) -> pd.Series:
    out = pd.Series([default]*len(df), index=df.index, dtype="float64")
    for c in cols:
        if c in df.columns:
            out = out.where(~out.isna(), pd.to_numeric(df[c], errors="coerce"))
    return out

def _estimate_ab(pa: pd.Series, bb_rate: Optional[pd.Series]) -> pd.Series:
    pa = pd.to_numeric(pa, errors="coerce").fillna(0.0)
    r = _as_rate(bb_rate).fillna(0.08) if bb_rate is not None else pd.Series([0.08]*len(pa), index=pa.index)
    # assume ~2% HBP and ~2% other non-AB PA; clamp AB share at least 0.5
    return (1.0 - (r + 0.02 + 0.02)).clip(lower=0.5) * pa

def _poisson_tail_at_least_k(mu: pd.Series, k: int) -> pd.Series:
    mu = pd.to_numeric(mu, errors="coerce").fillna(0.0)
    if k<=0: return pd.Series(1.0, index=mu.index)
    if k==1: return 1.0 - np.exp(-mu)
    if k==2: return 1.0 - np.exp(-mu)*(1.0+mu)
    out = []
    for m in mu:
        if m<=0:
            out.append(0.0); continue
        s=0.0; term=1.0
        for i in range(k):
            if i>0: term *= m/i
            s += term
        out.append(1.0 - math.exp(-m)*s)
    return pd.Series(out, index=mu.index, dtype=float)

def _binom_one_or_more(n: pd.Series, p: pd.Series) -> pd.Series:
    n = pd.to_numeric(n, errors="coerce").fillna(0.0)
    p = _as_rate(p).fillna(0.0).clip(0.0, 1.0-1e-12)
    return 1.0 - np.power(1.0 - p, n)

def _load_inputs() -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    z = _read_first_existing(IN_CANDIDATES)
    if z is None or z.empty:
        raise SystemExit("❌ Could not find batter_props_z_expanded.csv in expected locations.")
    bats = _read_first_existing(BATTERS_CANDIDATES)
    return _std(z), (None if bats is None or bats.empty else _std(bats))

def _first_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for n in candidates:
        if n.lower() in lower: return lower[n.lower()]
    return None

def _backfill_from_batters(z: pd.DataFrame, bats: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing PA / AVG / ISO / HR-rate in `z` using batters.csv (join on player_id).
    Never overwrites non-null values. Handles alias differences safely.
    """
    if "player_id" not in z.columns or "player_id" not in bats.columns:
        return z

    b = bats.copy()
    pa_c  = _first_col(b, ["pa","plate_appearances"])
    ab_c  = _first_col(b, ["ab","at_bats"])
    avg_c = _first_col(b, ["avg","ba","batting_avg"])
    iso_c = _first_col(b, ["iso","isolated_power"])
    hr_c  = _first_col(b, ["hr","home_run","home_runs"])

    keep_cols = ["player_id"]
    for c in [pa_c, ab_c, avg_c, iso_c, hr_c]:
        if c: keep_cols.append(c)
    b = b[keep_cols].copy()

    # derive hr_rate_pa if possible
    if pa_c and hr_c:
        b["hr_rate_pa"] = (pd.to_numeric(b[hr_c], errors="coerce") /
                           pd.to_numeric(b[pa_c], errors="coerce")).replace([np.inf,-np.inf], np.nan)
    else:
        b["hr_rate_pa"] = np.nan

    z = z.merge(b, on="player_id", how="left", suffixes=("", "_bats"))

    def fill_first(targets: list[str], src_col: Optional[str]):
        if not src_col or src_col not in z.columns: return
        for t in targets:
            if t in z.columns:
                z[t] = z[t].where(z[t].notna(), z[src_col])
                return
        # if none exist, create the first target from source
        z[targets[0]] = z[src_col]

    # backfill
    fill_first(["proj_pa","pa"], pa_c)
    fill_first(["proj_avg","proj_ba","avg","ba"], avg_c)
    fill_first(["proj_iso","iso"], iso_c)

    if "proj_hr_rate" not in z.columns:
        z["proj_hr_rate"] = np.nan
    z["proj_hr_rate"] = z["proj_hr_rate"].where(z["proj_hr_rate"].notna(), z["hr_rate_pa"])

    return z

# ---------- Slate completion helpers ----------

def _load_slate_maps() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (team_game_map: [team_id, game_id], team_labels: [team_id, team_abbrev])."""
    tgn = _read_csv_force_str(TGN_CSV)
    need = {"game_id","home_team_id","away_team_id","home_team","away_team"}
    miss = sorted(list(need - set(tgn.columns)))
    if miss:
        raise RuntimeError(f"{TGN_CSV} missing columns: {miss}")

    # Normalize numeric ids
    for c in ["game_id","home_team_id","away_team_id"]:
        tgn[c] = pd.to_numeric(tgn[c], errors="coerce")

    # Explode to 2 rows per game for (team_id, game_id)
    tg_home = tgn.rename(columns={"home_team_id":"team_id"})[["game_id","team_id"]]
    tg_away = tgn.rename(columns={"away_team_id":"team_id"})[["game_id","team_id"]]
    team_game_map = pd.concat([tg_home, tg_away], ignore_index=True).dropna().drop_duplicates()

    # Labels (use the abbrev columns the file already has)
    lab_home = tgn.rename(columns={"home_team":"team","home_team_id":"team_id"})[["team_id","team"]]
    lab_away = tgn.rename(columns={"away_team":"team","away_team_id":"team_id"})[["team_id","team"]]
    team_labels = pd.concat([lab_home, lab_away], ignore_index=True).dropna().drop_duplicates()

    return team_game_map, team_labels

def _augment_missing_from_lineups(df: pd.DataFrame, team_game_map: pd.DataFrame, team_labels: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure every slate side has batters:
      - Take players from data/raw/lineups.csv that aren't already present.
      - Attach game_id via team_game_map.
      - Provide minimal fields (player_id, team label, game_id) so downstream can proceed.
    """
    if not LINEUPS_CSV.exists():
        return df

    li = _read_csv_force_str(LINEUPS_CSV)
    if "player_id" not in li.columns:
        return df

    # Need team_id in lineups
    if "team_id" not in li.columns:
        return df

    # Normalize types
    li["player_id"] = pd.to_numeric(li["player_id"], errors="coerce")
    li["team_id"]   = pd.to_numeric(li["team_id"], errors="coerce")

    # Attach game_id for today (inner join to slate)
    li = li.merge(team_game_map, on="team_id", how="inner")  # only slate teams
    # Team abbrev/label for readability if missing
    li = li.merge(team_labels, on="team_id", how="left")

    # Existing players in df
    have = set(pd.to_numeric(df.get("player_id", pd.Series(dtype=float)), errors="coerce").dropna().astype(int).tolist())
    li["player_id"] = pd.to_numeric(li["player_id"], errors="coerce").astype("Int64")
    missing = li[~li["player_id"].isin(have)].copy()

    if missing.empty:
        return df

    # Build minimal rows for missing players
    add = pd.DataFrame({
        "player_id": missing["player_id"],
        "team": missing.get("team", pd.Series([""]*len(missing))),
        "game_id": missing["game_id"],
        # keep any other columns that may exist in df to preserve schema later
    })

    # Union and drop duplicates on player_id (prefer original df rows)
    base = df.copy()
    if "player_id" in base.columns:
        base_ids = set(pd.to_numeric(base["player_id"], errors="coerce").dropna().astype(int).tolist())
        add = add[~add["player_id"].astype("Int64").isin(base_ids)]
    df_out = pd.concat([base, add], ignore_index=True)

    # Report what we added
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    added = add[["player_id","team","game_id"]].copy()
    if len(added) > 0:
        added.to_csv(SUM_DIR / "batter_props_added_from_lineups.csv", index=False)

    return df_out

# ---------- Core projection ----------

def project_batter_props() -> pd.DataFrame:
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    z, bats = _load_inputs()

    # Optional: backfill from season priors for any missing rates
    if bats is not None:
        z = _backfill_from_batters(z, bats)

    # SLATE COMPLETION: ensure every slate side has batters (when z-feed is missing players)
    team_game_map, team_labels = _load_slate_maps()
    z = _augment_missing_from_lineups(z, team_game_map, team_labels)

    # ---- Derive projection inputs ----
    pa = _coalesce(z, "proj_pa", "pa").fillna(4.3)

    bb_rate = None
    for name in ["proj_bb_rate","bb_rate","bb_percent","bb%"]:
        if name in z.columns:
            bb_rate = z[name]; break

    ab = _estimate_ab(pa, bb_rate)

    p_hit_ab = _coalesce(z, "proj_avg","proj_ba","ba","avg").clip(lower=0.0)
    p_hit_ab = p_hit_ab.where(p_hit_ab < 1, 1.0 - 1e-12)

    p_hr_pa = None
    for name in ["proj_hr_rate","hr_rate","xhr_rate"]:
        if name in z.columns:
            p_hr_pa = _as_rate(z[name]); break
    if p_hr_pa is None:
        iso_for_fallback = _coalesce(z, "proj_iso","iso").fillna(0.120)
        p_hr_pa = (iso_for_fallback * 0.3).clip(0.0, 0.15)

    bb_r = _as_rate(bb_rate) if bb_rate is not None else pd.Series([0.08]*len(z), index=z.index)
    ab_over_pa = (1.0 - (bb_r + 0.02 + 0.02)).clip(lower=0.5)
    p_hr_ab = (p_hr_pa / ab_over_pa).clip(lower=0.0).where(lambda s: s<1, 1.0-1e-12)

    iso = _coalesce(z, "proj_iso","iso").fillna(0.120).clip(lower=0.0)
    extra_from_hr = 3.0 * p_hr_ab
    rem_extra = (iso - extra_from_hr).clip(lower=0.0)
    p_double_ab = (rem_extra / 1.2).clip(lower=0.0)
    p_triple_ab = (0.1 * rem_extra / 1.2).clip(lower=0.0)

    p_single_ab = (p_hit_ab - (p_double_ab + p_triple_ab + p_hr_ab)).clip(lower=0.0)
    over_sum = p_single_ab + p_double_ab + p_triple_ab + p_hr_ab
    if (over_sum > 1.0).any():
        scale = 0.999 / over_sum
        p_single_ab *= scale

    mu_hits = ab * p_hit_ab
    prob_hits_over_1p5 = _poisson_tail_at_least_k(mu_hits, 2)

    mu_s  = ab * p_single_ab
    mu_xb = ab * (p_double_ab + p_triple_ab + p_hr_ab)
    prob_tb_over_1p5 = 1.0 - np.exp(-(mu_s + mu_xb)) * (1.0 + mu_s)

    prob_hr_over_0p5 = _binom_one_or_more(pa, p_hr_pa)

    # ---- Build outputs ----
    cols_keep = [c for c in ["player_id","name","team","game_id","date"] if c in z.columns]
    out = pd.DataFrame(index=z.index)
    for c in cols_keep:
        out[c] = z[c]

    out["proj_pa_used"]         = pa
    out["proj_ab_est"]          = ab
    out["proj_avg_used"]        = p_hit_ab
    out["proj_iso_used"]        = iso
    out["proj_hr_rate_pa_used"] = p_hr_pa

    out["prob_hits_over_1p5"] = prob_hits_over_1p5.astype(float)
    out["prob_tb_over_1p5"]   = prob_tb_over_1p5.astype(float)
    out["prob_hr_over_0p5"]   = prob_hr_over_0p5.astype(float)

    # ---- Write *_final outputs expected downstream ----
    out.to_csv(OUT_PROJ_FINAL, index=False)

    # Minimal expanded file so batter-events can always merge env adjustments.
    # It only needs (player_id, game_id) + optional adj_woba_*; we default to 1.0.
    exp = out[["player_id"]].copy()
    exp["game_id"] = pd.to_numeric(out.get("game_id", pd.Series([np.nan]*len(out))), errors="coerce")
    exp["adj_woba_weather"]  = 1.0
    exp["adj_woba_park"]     = 1.0
    exp["adj_woba_combined"] = 1.0
    exp.to_csv(OUT_EXP_FINAL, index=False)

    # ---- Simple run summary ----
    def _missing_rate(s: pd.Series) -> float:
        return float(pd.isna(s).mean()) if len(s) else 0.0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✅ Wrote {len(out)} rows -> {OUT_PROJ_FINAL}")
    print(f"✅ Wrote {len(exp)} rows -> {OUT_EXP_FINAL}")
    print("Backfill summary (share missing BEFORE backfill):", {
        "pa": _missing_rate(_coalesce(z, "proj_pa","pa")),
        "avg": _missing_rate(_coalesce(z, "proj_avg","proj_ba","ba","avg")),
        "iso": _missing_rate(_coalesce(z, "proj_iso","iso")),
        "hr_rate": _missing_rate(_coalesce(z, "proj_hr_rate","hr_rate")),
    })
    return out

if __name__ == "__main__":
    project_batter_props()
