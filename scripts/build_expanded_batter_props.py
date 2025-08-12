# scripts/build_expanded_batter_props.py
import math
import sys
from pathlib import Path
import pandas as pd
from scipy.stats import zscore

# ------------------ STRICT MODE B ------------------
# Weather is REQUIRED. Fail fast if inputs/columns are missing.
INPUT_FILE  = Path("data/_projections/batter_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_z_expanded.csv")

WEATHER_INPUT   = Path("data/weather_input.csv")        # must have: home_team, Park Factor
WEATHER_ADJUST  = Path("data/weather_adjustments.csv")  # must have: home_team, weather_factor

REQ_INPUT_COLS = [
    "player_id","name","team",
    "pa","g","ab","slg",
    "season_hits","season_tb","season_hr","season_bb","season_k"
]

REQ_WEATHER_INPUT_COLS  = ["home_team","Park Factor"]
REQ_WEATHER_ADJUST_COLS = ["home_team","weather_factor"]

# ------------------ utils ------------------
def fail(msg):
    print(f"❌ {msg}")
    sys.exit(1)

def _coerce_float_series(s: pd.Series, name: str) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce")
    non_numeric = out.isna().sum()
    if non_numeric > 0:
        frac = non_numeric / max(1, len(out))
        if frac > 0.01:
            sample = s[out.isna()].head(5).tolist()
            fail(f"Column '{name}' has {non_numeric}/{len(out)} non-numeric values (>1%). Sample: {sample}")
        # else: allow sparse NaNs; caller should handle/drop
    return out

def _poisson_p_ge(k: int, lam: float) -> float:
    if lam is None or lam <= 0:
        return 0.0 if k > 0 else 1.0
    if k == 1:
        return 1.0 - math.exp(-lam)
    if k == 2:
        return 1.0 - math.exp(-lam) * (1.0 + lam)
    # Generic tail (rare)
    # compute CDF up to k-1, then 1-CDF
    term = math.exp(-lam)
    cdf = term
    n = 0
    while n < (k - 1) and term > 1e-12 and n < 500:
        n += 1
        term *= lam / n
        cdf += term
    return max(0.0, 1.0 - cdf)

def _clip(x, lo, hi):
    return max(lo, min(hi, x))

# ------------------ load & validate core ------------------
if not INPUT_FILE.exists():
    fail(f"Missing input file: {INPUT_FILE}")

df = pd.read_csv(INPUT_FILE)
df.columns = df.columns.str.strip()

# case-insensitive presence check, then realign to canonical names
lower_map = {c.lower(): c for c in df.columns}
missing = [c for c in REQ_INPUT_COLS if c.lower() not in lower_map]
if missing:
    fail(f"{INPUT_FILE} missing required columns: {missing}")

# Build canonical DataFrame with exact required names
canon = {}
for c in REQ_INPUT_COLS:
    canon[c] = df[lower_map[c.lower()]]
df = pd.DataFrame(canon)

# Drop completely empty rows and trim whitespace
df["team"] = df["team"].astype(str).str.strip()
df["name"] = df["name"].astype(str).str.strip()

# Coerce numeric columns
for num_col in ["pa","g","ab","slg","season_hits","season_tb","season_hr","season_bb","season_k"]:
    df[num_col] = _coerce_float_series(df[num_col], num_col)

# Drop rows with invalid opportunity
pre = len(df)
df = df[(df["pa"] > 0) & (df["g"] > 0)].copy()
dropped = pre - len(df)
if dropped > 0:
    print(f"ℹ️ Dropped {dropped} players with non-positive PA or G. Remaining: {len(df)}")

if df.empty:
    fail("No valid batter rows after validation.")

# ------------------ load & validate weather (STRICT) ------------------
if not WEATHER_INPUT.exists():
    fail(f"Missing weather input file (required in Mode B): {WEATHER_INPUT}")
if not WEATHER_ADJUST.exists():
    fail(f"Missing weather adjustments file (required in Mode B): {WEATHER_ADJUST}")

wi = pd.read_csv(WEATHER_INPUT)
wa = pd.read_csv(WEATHER_ADJUST)

# Ensure required columns exist exactly (case-sensitive allowed via mapping)
def _need_cols(df, path, required):
    m = {c.lower(): c for c in df.columns}
    miss = [c for c in required if c.lower() not in m]
    if miss:
        fail(f"{path} missing required columns: {miss}")
    return {c: m[c.lower()] for c in required}

wi_map = _need_cols(wi, WEATHER_INPUT, REQ_WEATHER_INPUT_COLS)
wa_map = _need_cols(wa, WEATHER_ADJUST, REQ_WEATHER_ADJUST_COLS)

# Normalize and build mappings (strict coverage)
wi_norm = wi[[wi_map["home_team"], wi_map["Park Factor"]]].copy()
wi_norm.columns = ["home_team","park_factor"]
wi_norm["home_team"] = wi_norm["home_team"].astype(str).str.strip()
wi_norm["park_factor"] = _coerce_float_series(wi_norm["park_factor"], "Park Factor")

wa_norm = wa[[wa_map["home_team"], wa_map["weather_factor"]]].copy()
wa_norm.columns = ["home_team","weather_factor"]
wa_norm["home_team"] = wa_norm["home_team"].astype(str).str.strip()
wa_norm["weather_factor"] = _coerce_float_series(wa_norm["weather_factor"], "weather_factor")

park_map = dict(zip(wi_norm["home_team"], wi_norm["park_factor"]))
weather_map = dict(zip(wa_norm["home_team"], wa_norm["weather_factor"]))

teams_in_df = set(df["team"].unique())
missing_pf = sorted([t for t in teams_in_df if t not in park_map])
missing_wf = sorted([t for t in teams_in_df if t not in weather_map])
if missing_pf:
    fail(f"weather_input.csv lacks Park Factor for teams: {missing_pf}")
if missing_wf:
    fail(f"weather_adjustments.csv lacks weather_factor for teams: {missing_wf}")

# ------------------ per-game lambdas ------------------
# PA per game
pa_pg = df["pa"] / df["g"]

# Per-PA rates
hit_rate = df["season_hits"] / df["pa"]
hr_rate  = df["season_hr"]   / df["pa"]
bb_rate  = df["season_bb"]   / df["pa"]
k_rate   = df["season_k"]    / df["pa"]

# Per-game expectations
lam_hits = hit_rate * pa_pg
lam_hr   = hr_rate  * pa_pg
lam_bb   = bb_rate  * pa_pg
lam_k    = k_rate   * pa_pg
lam_tb   = df["season_tb"] / df["g"]  # directly per-game from season TB

# Apply park/weather scaling (strict deterministic)
# Park Factor: 100 = neutral → multiplier around 1.0
def _pf_mult(pf_val):
    m = float(pf_val) / 100.0
    return _clip(m, 0.7, 1.3)

def _wf_mult(wf_val):
    return _clip(float(wf_val), 0.7, 1.3)

pf_series = df["team"].map(park_map).map(_pf_mult)
wf_series = df["team"].map(weather_map).map(_wf_mult)

# Exponents tuned by market
hits_mult = (pf_series ** 0.3) * (wf_series ** 0.4)
tb_mult   = (pf_series ** 0.7) * (wf_series ** 0.7)
hr_mult   = (pf_series ** 0.8) * (wf_series ** 0.8)
bb_mult   = 1.0
k_mult    = (wf_series ** 0.2)

lam_hits = (lam_hits * hits_mult).clip(lower=0.0, upper=6.0)
lam_tb   = (lam_tb   * tb_mult).clip(lower=0.0, upper=10.0)
lam_hr   = (lam_hr   * hr_mult).clip(lower=0.0, upper=2.0)
lam_bb   = (lam_bb   * bb_mult).clip(lower=0.0, upper=4.0)
lam_k    = (lam_k    * k_mult).clip(lower=0.0, upper=6.0)

# ------------------ build rows ------------------
rows = []

for i, r in df.iterrows():
    pid = r["player_id"]; nm = r["name"]; tm = r["team"]
    lh = float(lam_hits.iloc[i])
    ltb = float(lam_tb.iloc[i])
    lhr = float(lam_hr.iloc[i])
    lbb = float(lam_bb.iloc[i])
    lk = float(lam_k.iloc[i])

    # hits: lines 0.5 and 1.5
    for line in (0.5, 1.5):
        kreq = int(math.ceil(line))
        prob = _poisson_p_ge(kreq, lh)
        rows.append({
            "player_id": pid, "name": nm, "team": tm,
            "prop_type": "hits", "line": line,
            "projection": round(lh, 3),
            "over_probability": round(prob, 4),
        })

    # total bases: 1.5
    prob_tb = _poisson_p_ge(2, ltb)
    rows.append({
        "player_id": pid, "name": nm, "team": tm,
        "prop_type": "total_bases", "line": 1.5,
        "projection": round(ltb, 3),
        "over_probability": round(prob_tb, 4),
    })

    # home runs: 0.5
    prob_hr = 1.0 - math.exp(-lhr)
    rows.append({
        "player_id": pid, "name": nm, "team": tm,
        "prop_type": "home_runs", "line": 0.5,
        "projection": round(lhr, 3),
        "over_probability": round(prob_hr, 4),
    })

    # walks: 0.5
    prob_bb = 1.0 - math.exp(-lbb)
    rows.append({
        "player_id": pid, "name": nm, "team": tm,
        "prop_type": "walks", "line": 0.5,
        "projection": round(lbb, 3),
        "over_probability": round(prob_bb, 4),
    })

    # batter strikeouts: 0.5
    prob_k = 1.0 - math.exp(-lk)
    rows.append({
        "player_id": pid, "name": nm, "team": tm,
        "prop_type": "strikeouts", "line": 0.5,
        "projection": round(lk, 3),
        "over_probability": round(prob_k, 4),
    })

expanded = pd.DataFrame(rows)

# z-score by prop_type+line (diagnostic only)
if not expanded.empty:
    expanded["ultimate_z"] = expanded.groupby(["prop_type","line"])["projection"].transform(
        lambda s: zscore(s, nan_policy="omit")
    ).fillna(0.0).round(4)
else:
    expanded["ultimate_z"] = []

# final ordering
final = expanded[[
    "player_id","name","team","prop_type","line","projection","ultimate_z","over_probability"
]].sort_values(by=["name","prop_type","line"]).reset_index(drop=True)

# write
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Wrote per-game batter props (STRICT Mode B): {OUTPUT_FILE}")
print(f"   Rows: {len(final)} | Players input: {len(df)}")
