#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
import math

BATTER_IN   = Path("data/bets/prep/batter_props_bets.csv")
SCHED_IN    = Path("data/bets/mlb_sched.csv")
PITCHER_IN  = Path("data/bets/prep/pitcher_props_bets.csv")
# Optional projections (used if present to compute better lambda per prop)
PROJ_IN     = Path("data/_projections/batter_props_projected.csv")

OUT_FILE    = Path("data/bets/prep/batter_props_final.csv")

# ---------------- helpers ----------------
def _norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _to_scalar(x):
    """Return a single scalar from possible Series/array/list/scalar; NaN if empty."""
    if isinstance(x, pd.Series):
        x = x.dropna()
        return x.iloc[0] if not x.empty else np.nan
    if isinstance(x, (list, tuple, np.ndarray)):
        arr = np.asarray(x).ravel()
        return arr[0] if arr.size else np.nan
    return x

def poisson_tail_over(line_val: float, lam: float) -> float:
    """
    P(X > line) for Poisson(λ) with fractional sportsbook lines:
      threshold = floor(line) + 1  -> P(X >= threshold) = 1 - P(X <= threshold-1)
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
    # P(X <= k) = sum_{i=0..k} e^-λ λ^i / i!
    term = math.exp(-lam)
    acc = term
    for i in range(1, thr):
        term *= lam / i
        acc += term
        if term < 1e-15:  # early break for stability
            break
    return float(max(0.0, min(1.0, 1.0 - acc)))

def ensure_columns(df: pd.DataFrame, spec: dict) -> list:
    """
    Ensure columns exist and coerce dtypes.
    spec: {col: ('str'|'num', default_val)}
    """
    created = []
    for col, (kind, default) in spec.items():
        if col not in df.columns:
            df[col] = default
            created.append(col)
        if kind == "str":
            df[col] = _norm(df[col].astype(str))
        elif kind == "num":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return created

def ensure_prop(df: pd.DataFrame, when: str) -> None:
    """Guarantee df['prop'] exists; do not overwrite if already present."""
    if "prop" in df.columns:
        df["prop"] = _norm(df["prop"].astype(str))
        return
    if "prop_type" in df.columns:
        df["prop"] = _norm(df["prop_type"].astype(str))
        print(f"ℹ️ [{when}] Created 'prop' from 'prop_type'.")
    else:
        df["prop"] = ""
        print(f"⚠️ [{when}] Created empty 'prop' (no 'prop' or 'prop_type').")

# ---------------- load ----------------
if not BATTER_IN.exists():
    raise SystemExit(f"❌ Missing {BATTER_IN}")
if not SCHED_IN.exists():
    raise SystemExit(f"❌ Missing {SCHED_IN}")
if not PITCHER_IN.exists():
    raise SystemExit(f"❌ Missing {PITCHER_IN}")

bat = pd.read_csv(BATTER_IN)
sch = pd.read_csv(SCHED_IN)
pit = pd.read_csv(PITCHER_IN)

# basic hygiene
bat.columns = [c.strip() for c in bat.columns]
sch.columns = [c.strip() for c in sch.columns]
pit.columns = [c.strip() for c in pit.columns]

# ---- make sure 'prop' exists right after load (no overwrite if present)
ensure_prop(bat, when="post-load")

# ensure other key cols/types
created = ensure_columns(bat, {
    "player_id": ("str", ""),
    "name":      ("str", ""),
    "team":      ("str", ""),
    "prop":      ("str", ""),   # re-ensure string type
    "line":      ("num", np.nan),
    "value":     ("num", np.nan),
})
if created:
    print(f"ℹ️ Auto-created/normalized batter columns: {', '.join(created)}")

print(f"📊 Batter rows (input): {len(bat)}")
try:
    print(f"🧱 Prop distribution (input): {bat['prop'].value_counts(dropna=False).to_dict()}")
except Exception:
    pass

# ---------------- schedule -> opp team ----------------
need = [c for c in ("home_team", "away_team") if c not in sch.columns]
if need:
    raise SystemExit(f"❌ schedule missing columns: {need}")

sch_home = sch[["home_team", "away_team"]].copy()
sch_home.columns = ["team", "opp_team"]
sch_away = sch[["away_team", "home_team"]].copy()
sch_away.columns = ["team", "opp_team"]
team_pairs = pd.concat([sch_home, sch_away], ignore_index=True)
team_pairs["team"] = _norm(team_pairs["team"])
team_pairs["opp_team"] = _norm(team_pairs["opp_team"])

bat = bat.merge(team_pairs, on="team", how="left")

# ---------------- pick 1 pitcher per team ----------------
if "team" not in pit.columns:
    raise SystemExit("❌ pitcher file missing 'team' column")

pit["team"] = _norm(pit["team"])
if "mega_z" in pit.columns:
    pit_one = (
        pit.sort_values(by=["team", "mega_z"], ascending=[True, False])
           .groupby("team", as_index=False)
           .head(1)
    )
else:
    pit_one = pit.groupby("team", as_index=False).head(1)

# IMPORTANT: exclude pitcher's 'prop' to avoid clobbering batter 'prop'
keep_pit_cols = [c for c in
    ["team", "name",               # <- keep
     # "prop",                     # <- DO NOT keep; prevents prop_x/prop_y collision
     "mega_z", "z_score",
     "over_probability", "line", "value"]
    if c in pit_one.columns]

pit_one = pit_one[keep_pit_cols].rename(columns={
    "team":  "opp_team",
    "name":  "opp_pitcher_name",
    "mega_z":"opp_pitcher_mega_z",
    "z_score":"opp_pitcher_z",
    "over_probability":"opp_pitcher_over_prob",
    "line":  "opp_pitcher_line",
    "value": "opp_pitcher_value",
})

pre_merge_rows = len(bat)
bat = bat.merge(pit_one, on="opp_team", how="left")
matched_rows = bat["opp_pitcher_name"].notna().sum()
unmatched_rows = pre_merge_rows - matched_rows
print(f"🧩 Pitcher match: matched={matched_rows} unmatched={unmatched_rows} (of {pre_merge_rows})")
if unmatched_rows:
    top_unmatched = (
        bat[bat["opp_pitcher_name"].isna()]
        .groupby("opp_team", dropna=False)
        .size()
        .sort_values(ascending=False)
        .head(8)
        .to_dict()
    )
    print(f"🔎 Unmatched by opp_team (top): {top_unmatched}")

# ---- re-assert 'prop' exists (in case earlier merges altered columns)
ensure_prop(bat, when="pre-zscores")

# Drop blank-prop rows only if some are blank (but not all)
blank_mask = bat["prop"].eq("")
blank_count = int(blank_mask.sum())
if blank_count and not blank_mask.all():
    before = len(bat)
    bat = bat[~blank_mask].copy()
    after = len(bat)
    print(f"⚠️ Removed {blank_count} rows with blank prop before z-scores (kept {after}/{before}).")
elif blank_mask.all():
    print("⚠️ All rows have blank 'prop' — keeping rows (no drop).")

# ensure numeric for calc
bat["value"] = pd.to_numeric(bat.get("value", np.nan), errors="coerce")
bat["line"]  = pd.to_numeric(bat.get("line",  np.nan), errors="coerce")

# ---------------- batter z per prop ----------------
def _z_transform(s: pd.Series) -> pd.Series:
    sd = s.std(ddof=0)
    if sd == 0 or not np.isfinite(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / sd

bat["batter_z"] = (
    bat.groupby("prop", dropna=False)["value"]
       .transform(_z_transform)
       .astype(float)
)

# ---------------- mega_z (adjust for opp pitcher) ----------------
W = 0.5
bat["opp_pitcher_mega_z"] = pd.to_numeric(bat.get("opp_pitcher_mega_z", np.nan), errors="coerce")
bat["mega_z"] = bat["batter_z"] - W * bat["opp_pitcher_mega_z"].fillna(0.0)

# ---------------- optional projections join (for better lambda) ----------------
if PROJ_IN.exists():
    try:
        proj = pd.read_csv(PROJ_IN)
        proj.columns = proj.columns.str.strip().str.lower()
        # De-duplicate any repeated column names (keep last occurrence)
        proj = proj.loc[:, ~proj.columns.duplicated(keep="last")]
        # prefer join by player_id if populated on both sides; else (name, team)
        use_id = ("player_id" in bat.columns and "player_id" in proj.columns and
                  bat["player_id"].notna().any() and proj["player_id"].notna().any())
        key = ["player_id"] if use_id else ["name", "team"]
        for k in key:
            if k in bat.columns:  bat[k]  = _norm(bat[k])
            if k in proj.columns: proj[k] = _norm(proj[k])
        keep = [c for c in ["player_id","name","team","ab","proj_hits","proj_hr","proj_slg"] if c in proj.columns]
        proj_slim = proj[keep].drop_duplicates()
        bat = bat.merge(proj_slim, on=[k for k in key if k in proj_slim.columns], how="left", suffixes=("", "_proj"))
        print(f"🧮 Projections join: columns present -> {', '.join([c for c in ['ab','proj_hits','proj_hr','proj_slg'] if c in bat.columns])}")
    except Exception as e:
        print(f"⚠️ Projections not used: {e}")

# ---------------- over_probability ----------------
def _lambda_for_row(row) -> float:
    """
    Choose λ per row:
      - If projections present: use prop-specific λ (proj_hits/proj_hr/proj_slg*AB)
      - Else fallback to original 'value'.
    Guardrails reject absurd projection values and scale down unreasonable fallbacks.
    """
    prop = str(_to_scalar(row.get("prop", ""))).lower()

    proj_hits = _to_scalar(row.get("proj_hits", np.nan))
    proj_hr   = _to_scalar(row.get("proj_hr",   np.nan))
    proj_slg  = _to_scalar(row.get("proj_slg",  np.nan))
    ab        = _to_scalar(row.get("ab",        np.nan))
    val       = _to_scalar(row.get("value",     np.nan))

    def ok(x, lo, hi):
        try:
            xf = float(x)
            return (pd.notna(xf)) and (lo <= xf <= hi)
        except Exception:
            return False

    # HITS: λ should be ~0–6
    if prop == "hits":
        if ok(proj_hits, 0.0, 6.0):
            return float(proj_hits)
        # Heuristic fallback from SLG & AB → estimate AVG ≈ SLG / 1.6
        if pd.notna(proj_slg) and pd.notna(ab) and ok(proj_slg, 0.1, 1.2) and ok(ab, 0.0, 7.0):
            est_avg = max(0.0, min(1.0, float(proj_slg) / 1.6))
            lam = est_avg * float(ab)
            return max(0.0, min(6.0, lam))
        # Last resort: scale your 'value' down if it looks like a huge index
        if pd.notna(val) and float(val) > 6:
            return float(val) / 30.0
        return float(val) if pd.notna(val) else np.nan

    # HOME RUNS: λ should be ~0–2
    if prop == "home_runs":
        if ok(proj_hr, 0.0, 2.0):
            return float(proj_hr)
        if pd.notna(val) and float(val) > 2:
            return float(val) / 50.0
        return float(val) if pd.notna(val) else np.nan

    # TOTAL BASES: use SLG * AB; cap to reasonable range
    if prop == "total_bases" and pd.notna(proj_slg) and pd.notna(ab):
        lam_tb = float(proj_slg) * float(ab)
        return max(0.0, min(12.0, lam_tb))

    return float(val) if pd.notna(val) else np.nan

def _over_prob(row):
    ln  = _to_scalar(row.get("line", np.nan))
    lam = _lambda_for_row(row)
    if pd.isna(ln) or pd.isna(lam):
        return np.nan
    try:
        p = poisson_tail_over(float(ln), float(lam))
        # Only clip to [0,1]; no artificial floor/ceiling
        return float(min(max(p, 0.0), 1.0))
    except Exception:
        return np.nan

bat["over_probability"] = bat.apply(_over_prob, axis=1)

# ---------------- order + save ----------------
out_cols = [
    "player_id","name","team","prop","line","value",
    "batter_z","mega_z","over_probability",
    "opp_team","opp_pitcher_name","opp_pitcher_mega_z","opp_pitcher_z"
]
out_cols = [c for c in out_cols if c in bat.columns]
out = bat[out_cols].copy()

OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(OUT_FILE, index=False)

print(f"✅ Wrote: {OUT_FILE} (rows={len(out)})")
try:
    print(f"📦 Prop distribution (output): {out['prop'].value_counts(dropna=False).to_dict()}")
except Exception:
    pass
matched_final = out["opp_pitcher_name"].notna().sum() if "opp_pitcher_name" in out.columns else 0
print(f"🧾 Opp pitcher populated on {matched_final} of {len(out)} output rows")
