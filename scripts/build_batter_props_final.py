# scripts/build_batter_props_final.py
import pandas as pd
import numpy as np
from pathlib import Path
import math

BATTER_IN   = Path("data/bets/prep/batter_props_bets.csv")
SCHED_IN    = Path("data/bets/mlb_sched.csv")
PITCHER_IN  = Path("data/bets/prep/pitcher_props_bets.csv")
OUT_FILE    = Path("data/bets/prep/batter_props_final.csv")

# -------- helpers --------
def _norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def poisson_p_ge(k: int, lam: float) -> float:
    """Tail P(X>=k) for Poisson(lam)."""
    if lam is None or lam <= 0:
        return 0.0 if k > 0 else 1.0
    if k == 1:
        return 1.0 - math.exp(-lam)
    if k == 2:
        return 1.0 - math.exp(-lam) * (1.0 + lam)
    term = math.exp(-lam)
    cdf = term
    n = 0
    while n < (k - 1) and term > 1e-12 and n < 200:
        n += 1
        term *= lam / n
        cdf += term
    return max(0.0, 1.0 - cdf)

def ensure_columns(df: pd.DataFrame, spec: dict) -> list:
    """
    Ensure columns exist and coerce types.
    spec: {col: ('str'|'num', default_val)}. Returns list of created cols.
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

# -------- load --------
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

# -------- make sure batter columns exist (auto-create if missing) --------
if "prop" not in bat.columns:
    if "prop_type" in bat.columns:
        bat["prop"] = bat["prop_type"]
        print("ℹ️ Created 'prop' from 'prop_type'.")
    else:
        bat["prop"] = ""
        print("⚠️ Created empty 'prop' (no 'prop' or 'prop_type').")

BAT_SPEC = {
    "player_id": ("str", ""),
    "name":      ("str", ""),
    "team":      ("str", ""),
    "prop":      ("str", ""),
    "line":      ("num", np.nan),
    "value":     ("num", np.nan),
}
created = ensure_columns(bat, BAT_SPEC)
if created:
    print(f"ℹ️ Auto-created/normalized batter columns: {', '.join(created)}")

# quick batch stats pre-merge
print(f"📊 Batter rows (input): {len(bat)}")
if "prop" in bat.columns:
    try:
        cnt = bat["prop"].value_counts(dropna=False).to_dict()
        print(f"📦 Prop distribution (input): {cnt}")
    except Exception:
        pass

# -------- find opponent team via schedule --------
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

# merge opponent team onto each batter row
bat = bat.merge(team_pairs, on="team", how="left")

# -------- pick the starting pitcher for each team --------
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

keep_pit_cols = [
    c for c in ["team", "name", "prop", "mega_z", "z_score",
                "over_probability", "line", "value"]
    if c in pit_one.columns
]
pit_one = pit_one[keep_pit_cols].rename(columns={
    "team":  "opp_team",
    "name":  "opp_pitcher_name",
    "mega_z":"opp_pitcher_mega_z",
    "z_score":"opp_pitcher_z",
    "over_probability":"opp_pitcher_over_prob",
    "line":  "opp_pitcher_line",
    "value": "opp_pitcher_value",
})

# attach opponent pitcher by opp_team
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

# -------- HARD SAFETY: guarantee 'prop' & 'value' before grouping --------
if "prop" not in bat.columns:
    if "prop_type" in bat.columns:
        bat["prop"] = bat["prop_type"].astype(str).str.strip()
        print("ℹ️ Recreated 'prop' from 'prop_type' (post-merge).")
    else:
        bat["prop"] = ""
        print("⚠️ Recreated empty 'prop' (post-merge).")
bat["prop"] = bat["prop"].fillna("").astype(str).str.strip()
bat["value"] = pd.to_numeric(bat.get("value", np.nan), errors="coerce")

# -------- compute batter z per prop (transform -> 1D Series) --------
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

# -------- combined mega_z --------
W = 0.5
bat["opp_pitcher_mega_z"] = pd.to_numeric(bat.get("opp_pitcher_mega_z", np.nan), errors="coerce")
bat["mega_z"] = bat["batter_z"] - W * bat["opp_pitcher_mega_z"].fillna(0.0)

# -------- over_probability --------
def _over_prob(row):
    val = row.get("value", np.nan)
    ln  = row.get("line", np.nan)
    if pd.isna(val) or pd.isna(ln):
        return np.nan
    try:
        k = int(math.ceil(float(ln)))
        lam = max(0.0, float(val))
        return round(max(0.02, min(0.98, poisson_p_ge(k, lam))), 4)
    except Exception:
        return np.nan

bat["over_probability"] = bat.apply(_over_prob, axis=1)

# -------- order + save --------
out_cols = [
    "player_id","name","team","prop","line","value",
    "batter_z","mega_z","over_probability",
    "opp_team","opp_pitcher_name","opp_pitcher_mega_z","opp_pitcher_z"
]
out_cols = [c for c in out_cols if c in bat.columns]
out = bat[out_cols].copy()

OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(OUT_FILE, index=False)

# final logs
print(f"✅ Wrote: {OUT_FILE} (rows={len(out)})")
if "prop" in out.columns:
    try:
        out_prop_cnt = out["prop"].value_counts(dropna=False).to_dict()
        print(f"📦 Prop distribution (output): {out_prop_cnt}")
    except Exception:
        pass
matched_final = out["opp_pitcher_name"].notna().sum() if "opp_pitcher_name" in out.columns else 0
print(f"🧾 Opp pitcher populated on {matched_final} of {len(out)} output rows")
