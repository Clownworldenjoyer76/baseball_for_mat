TEAM_ALIASES = {
    'RedSox': 'Red Sox',
    'WhiteSox': 'White Sox',
    'BlueJays': 'Blue Jays',
    'Diamondbacks': 'Diamondbacks',
    'Braves': 'Braves',
    'Cubs': 'Cubs',
    'Dodgers': 'Dodgers',
    'Mariners': 'Mariners',
    'Marlins': 'Marlins',
    'Nationals': 'Nationals',
    'Padres': 'Padres',
    'Phillies': 'Phillies',
    'Pirates': 'Pirates',
    'Rays': 'Rays',
    'Rockies': 'Rockies',
    'Tigers': 'Tigers',
    'Twins': 'Twins',
}

#!/usr/bin/env python3
# scripts/build_expanded_batter_props.py  (STRICT Mode B: weather required)
import math
from pathlib import Path
import pandas as pd
from scipy.stats import zscore

# ------------------ Inputs / Outputs ------------------
INPUT_FILE  = Path("data/tagged/batters_normalized.csv")  # switched to this source
OUTPUT_FILE = Path("data/_projections/batter_props_z_expanded.csv")

# STRICT weather mode (Mode B): both files & columns required
WEATHER_INPUT  = Path("data/weather_input.csv")       # must contain: home_team, Park Factor
WEATHER_ADJUST = Path("data/weather_adjustments.csv") # must contain: home_team, weather_factor

# ------------------ Validation helpers ------------------
REQ_CORE_COLS = [
    "player_id","name","team",
    "pa","g","ab","slg",
    "season_hits","season_tb","season_hr","season_bb","season_k"
]

def _require_columns(df: pd.DataFrame, cols: list[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{where} missing required columns: {missing}")

def _require_file(path: Path, desc: str):
    if not path.exists():
        raise SystemExit(f"Required file not found: {desc} → {path}")

def _coerce_float(s):
    try:
        return float(s)
    except Exception:
        return None

def _safe_div(a, b, default=0.0):
    a = _coerce_float(a); b = _coerce_float(b)
    if a is None or b is None or b == 0:
        return default
    return a / b

def _poisson_p_ge(k, lam):
    """P(X >= k) for Poisson(lambda)."""
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

def _clip(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo

# ------------------ Load & validate ------------------
_input_path = INPUT_FILE
_require_file(_input_path, "Batter source")
df = pd.read_csv(_input_path)
df.columns = df.columns.str.strip()

_require_columns(df, REQ_CORE_COLS, f"{_input_path}")

# enforce numeric types
for c in ["pa","g","ab","slg","season_hits","season_tb","season_hr","season_bb","season_k"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# drop rows with obviously unusable core fields
pre = len(df)
df = df.dropna(subset=["pa","g","ab"]).copy()
df = df[df["g"] > 0]
print(f"🔎 kept {len(df)}/{pre} batters after basic NA/zero checks")

# ------------------ Weather (STRICT) ------------------
_require_file(WEATHER_INPUT,  "Weather input (Park Factor)")
_require_file(WEATHER_ADJUST, "Weather adjustments (weather_factor)")

wi = pd.read_csv(WEATHER_INPUT)
wa = pd.read_csv(WEATHER_ADJUST)
wi.columns = wi.columns.str.strip()
wa.columns = wa.columns.str.strip()

_require_columns(wi, ["home_team","Park Factor"], "weather_input.csv")
_require_columns(wa, ["home_team","weather_factor"], "weather_adjustments.csv")

# Build team → park/weather dicts (no guessing; strings must match exactly)
park_by_team = {}
for _, r in wi[["home_team","Park Factor"]].dropna().iterrows():
    team = str(r["home_team"]).strip()
    pf   = _coerce_float(r["Park Factor"])
    if pf is None:
        raise SystemExit(f"Non-numeric Park Factor for team '{team}'")
    park_by_team[team] = _clip(pf/100.0, 0.7, 1.3)

weather_by_team = {}
for _, r in wa[["home_team","weather_factor"]].dropna().iterrows():
    team = str(r["home_team"]).strip()
    wf   = _coerce_float(r["weather_factor"])
    if wf is None:
        raise SystemExit(f"Non-numeric weather_factor for team '{team}'")
    weather_by_team[team] = _clip(wf, 0.7, 1.3)

# Verify every team in df exists in BOTH dicts
teams_in_df = set(df["team"].astype(str).str.strip().unique())
missing_pf = sorted([t for t in teams_in_df if t not in park_by_team])
missing_wf = sorted([t for t in teams_in_df if t not in weather_by_team])
if missing_pf or missing_wf:
    msg = []
    if missing_pf:
        msg.append(f"Park Factor missing for teams: {missing_pf}")
    if missing_wf:
        msg.append(f"weather_factor missing for teams: {missing_wf}")
    raise SystemExit(" | ".join(msg))

# ------------------ Per-game λ projections ------------------
# PA per game
pa_pg = df.apply(lambda r: _safe_div(r["pa"], r["g"], 4.2), axis=1)

# per-PA rates from season totals
hit_rate = df.apply(lambda r: _safe_div(r["season_hits"], r["pa"], 0.0), axis=1)
hr_rate  = df.apply(lambda r: _safe_div(r["season_hr"],   r["pa"], 0.0), axis=1)
bb_rate  = df.apply(lambda r: _safe_div(r["season_bb"],   r["pa"], 0.0), axis=1)
k_rate   = df.apply(lambda r: _safe_div(r["season_k"],    r["pa"], 0.0), axis=1)

# per-game expected values
hits_pg = hit_rate * pa_pg
hr_pg   = hr_rate  * pa_pg
bb_pg   = bb_rate  * pa_pg
k_pg    = k_rate   * pa_pg

# total bases per-game: prefer direct season_tb/g, else SLG*AB_pg
tb_pg_direct = df.apply(lambda r: _safe_div(r["season_tb"], r["g"], None), axis=1)
ab_pg = pa_pg * df.apply(lambda r: _safe_div(r["ab"], r["pa"], 0.9), axis=1)
tb_pg_slg = ab_pg * df["slg"].fillna(0.0)

tb_pg = tb_pg_direct.where(tb_pg_direct.notna(), tb_pg_slg)

# ------------------ Apply park & weather (strictly by df['team']) ------------------
def _apply_context(team, lam_hits, lam_tb, lam_hr, lam_bb, lam_k):
    pf = park_by_team[team]
    wf = weather_by_team[team]
    lam_hr   *= pow(pf, 0.8) * pow(wf, 0.8)
    lam_tb   *= pow(pf, 0.7) * pow(wf, 0.7)
    lam_hits *= pow(pf, 0.3) * pow(wf, 0.4)
    lam_k    *= pow(wf, 0.2)
    # cap ranges
    lam_hits = _clip(lam_hits, 0.0, 6.0)
    lam_tb   = _clip(lam_tb,   0.0, 10.0)
    lam_hr   = _clip(lam_hr,   0.0, 2.0)
    lam_bb   = _clip(lam_bb,   0.0, 4.0)
    lam_k    = _clip(lam_k,    0.0, 6.0)
    return lam_hits, lam_tb, lam_hr, lam_bb, lam_k

rows = []
for idx, r in df.iterrows():
    team = str(r["team"]).strip()
    lam_hits = float(hits_pg.iloc[idx])
    lam_tb   = float(tb_pg.iloc[idx])
    lam_hr   = float(hr_pg.iloc[idx])
    lam_bb   = float(bb_pg.iloc[idx])
    lam_k    = float(k_pg.iloc[idx])

    lam_hits, lam_tb, lam_hr, lam_bb, lam_k = _apply_context(team, lam_hits, lam_tb, lam_hr, lam_bb, lam_k)

    # Hits (0.5, 1.5)
    for line in (0.5, 1.5):
        k = int(math.ceil(line))
        prob = _poisson_p_ge(k, lam_hits)
        rows.append({
            "player_id": r["player_id"],
            "name": r["name"],
            "team": team,
            "prop_type": "hits",
            "line": line,
            "projection": round(lam_hits, 3),
            "over_probability": round(_clip(prob, 0.0, 0.9999), 4),
        })

    # Total bases (1.5)
    prob_tb = _poisson_p_ge(2, lam_tb)
    rows.append({
        "player_id": r["player_id"],
        "name": r["name"],
        "team": team,
        "prop_type": "total_bases",
        "line": 1.5,
        "projection": round(lam_tb, 3),
        "over_probability": round(_clip(prob_tb, 0.0, 0.9999), 4),
    })

    # Home runs (0.5)
    prob_hr = 1.0 - math.exp(-lam_hr)
    rows.append({
        "player_id": r["player_id"],
        "name": r["name"],
        "team": team,
        "prop_type": "home_runs",
        "line": 0.5,
        "projection": round(lam_hr, 3),
        "over_probability": round(_clip(prob_hr, 0.0, 0.9999), 4),
    })

    # Walks (0.5)
    prob_bb = 1.0 - math.exp(-lam_bb)
    rows.append({
        "player_id": r["player_id"],
        "name": r["name"],
        "team": team,
        "prop_type": "walks",
        "line": 0.5,
        "projection": round(lam_bb, 3),
        "over_probability": round(_clip(prob_bb, 0.0, 0.9999), 4),
    })

    # Strikeouts (batter, 0.5)
    prob_k = 1.0 - math.exp(-lam_k)
    rows.append({
        "player_id": r["player_id"],
        "name": r["name"],
        "team": team,
        "prop_type": "strikeouts",
        "line": 0.5,
        "projection": round(lam_k, 3),
        "over_probability": round(_clip(prob_k, 0.0, 0.9999), 4),
    })

expanded = pd.DataFrame(rows)

# z-score on per-game projections (diagnostic)
expanded["ultimate_z"] = expanded.groupby(["prop_type","line"])["projection"].transform(
    lambda s: zscore(s, nan_policy="omit")
).fillna(0.0).round(4)

# Final ordering / output
final = expanded[[
    "player_id","name","team","prop_type","line","projection","ultimate_z","over_probability"
]].sort_values(by=["name","prop_type","line"]).reset_index(drop=True)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote per-game batter props with STRICT weather: {OUTPUT_FILE}")
