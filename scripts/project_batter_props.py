# scripts/project_batter_props.py
# Corrected + adds upstream Poisson-based over probabilities for hits/HR/TB

from __future__ import annotations
import math
from pathlib import Path
import pandas as pd
from projection_formulas import calculate_all_projections

# ---------------- normalization & schedule constants ----------------
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None

FINAL_FILE   = Path("data/end_chain/final/bat_today_final.csv")
OUTPUT_FILE  = Path("data/_projections/batter_props_projected.csv")

SCHED_FILE   = Path("data/bets/mlb_sched.csv")
TEAMMAP_FILE = Path("data/Data/team_name_master.csv")
TZ_NAME      = "America/New_York"
# -------------------------------------------------------------------

# Accept your actual headers
ALIASES = {
    "PA": ["pa", "PA"],
    "BB%": ["bb_percent", "BB%"],
    "K%": ["k_percent", "K%"],
    "H/AB": ["batting_avg", "H/AB", "hits_per_ab", "AVG", "avg"],
    "HR/AB": ["HR/AB", "hr_per_ab"],
    "opp_K%": ["opp_K%", "opp_k_percent", "opponent_k_percent"],
    "opp_BB%": ["opp_BB%", "opp_bb_percent", "opponent_bb_percent"],
}

def _resolve(df: pd.DataFrame, target: str, required: bool) -> str | None:
    cands = ALIASES.get(target, [target])
    for cand in cands:
        if cand in df.columns:
            return cand
        for col in df.columns:
            if col.lower() == cand.lower():
                return col
    if required:
        raise ValueError(f"Missing required column for batters: {target} (accepted: {cands})")
    return None

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Only PA is strictly required — the rest can be derived in projection_formulas
    _resolve(df, "PA", required=True)
    _resolve(df, "BB%", required=False)
    _resolve(df, "K%", required=False)
    _resolve(df, "H/AB", required=False)
    _resolve(df, "HR/AB", required=False)
    _resolve(df, "opp_K%", required=False)
    _resolve(df, "opp_BB%", required=False)
    return df

# ---- Probability helpers (no 0.98 cap) ----
def _poisson_cdf_le(k: int, lam: float) -> float:
    """P(X <= k) for Poisson(λ)."""
    k = int(k)
    if lam <= 0:
        return 1.0 if k >= 0 else 0.0
    term = math.exp(-lam)
    acc = term
    for i in range(1, k + 1):
        term *= lam / i
        acc += term
    return min(max(acc, 0.0), 1.0)

def _poisson_over_prob(lam: float, line_val: float) -> float:
    """
    P(X > line) where 'line' is fractional (e.g., 0.5, 1.5).
    threshold = floor(line) + 1 -> P(X >= threshold) = 1 - P(X <= threshold-1)
    """
    try:
        thr = int(math.floor(float(line_val))) + 1
    except Exception:
        return float("nan")
    if thr <= 0:
        return 1.0
    return float(min(max(1.0 - _poisson_cdf_le(thr - 1, float(lam)), 0.0), 1.0))

# ---------------- helpers for normalization/schedule ----------------
def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df

def _today_str() -> str:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")

def _build_team_normalizer(team_map_df: pd.DataFrame):
    """
    Map any alias to canonical team_name (per team_name_master.csv).
    STRICT: unknown aliases become NaN (no fallback/guessing).
    """
    req = {"team_code", "team_name", "abbreviation", "clean_team_name"}
    miss = [c for c in req if c not in team_map_df.columns]
    if miss:
        raise SystemExit(f"❌ team_name_master.csv missing columns: {miss}")

    alias_to_team = {}
    def _add(key_val, team_name):
        if pd.isna(key_val): return
        k = str(key_val).strip().lower()
        if k: alias_to_team[k] = team_name

    for _, r in team_map_df.iterrows():
        canon = str(r["team_name"]).strip()
        _add(r["team_code"], canon)
        _add(r["abbreviation"], canon)
        _add(r["clean_team_name"], canon)
        _add(r["team_name"], canon)
        _add(str(r["team_name"]).lower(), canon)

    def normalize_series_strict(s: pd.Series) -> pd.Series:
        return s.astype(str).map(lambda x: alias_to_team.get(str(x).strip().lower(), pd.NA))

    return normalize_series_strict
# -------------------------------------------------------------------

def main():
    # Load the day’s batter base and ensure expected columns exist
    df = pd.read_csv(FINAL_FILE)
    df = _ensure_columns(df)

    # ---- normalize teams & filter to today's schedule (STRICT, no guesses) ----
    teammap = _std(pd.read_csv(TEAMMAP_FILE))
    normalize_series = _build_team_normalizer(teammap)

    # Normalize any team columns present
    for col in ["team", "Team", "team_abbr", "bat_team", "opp_team", "home_team", "away_team"]:
        if col in df.columns:
            orig = df[col].copy()
            df[col] = normalize_series(df[col])
            unknown = orig[pd.isna(df[col])].dropna().unique().tolist()
            if unknown:
                raise SystemExit(f"❌ Unknown team alias(es) in {FINAL_FILE} column '{col}': {unknown}")

    # Read schedule and select today (date-only, TZ=America/New_York)
    sched = _std(pd.read_csv(SCHED_FILE))
    need_sched = [c for c in ("home_team", "away_team", "date") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    for col in ["home_team", "away_team"]:
        orig = sched[col].copy()
        sched[col] = normalize_series(sched[col])
        unknown = orig[pd.isna(sched[col])].dropna().unique().tolist()
        if unknown:
            raise SystemExit(f"❌ Unknown team alias(es) in schedule '{col}': {unknown}")

    sched["date"] = pd.to_datetime(sched["date"], errors="coerce")
    try:
        sched["date"] = sched["date"].dt.tz_localize(None)
    except Exception:
        pass
    if sched["date"].isna().all():
        raise SystemExit("❌ schedule 'date' column is not parseable")

    today_date = pd.to_datetime(_today_str()).date()
    sched_today = sched[sched["date"].dt.date == today_date].copy()
    if sched_today.empty:
        latest = sched["date"].max()
        sched_today = sched[sched["date"] == latest].copy()
        print(f"⚠️ No schedule for today ({today_date}); using latest {latest.date()} instead.")
    else:
        print(f"✅ Using schedule for {today_date}")

    # Filter batter base to teams actually on today's slate
    today_teams = set(sched_today["home_team"]).union(set(sched_today["away_team"]))
    team_col = None
    for c in ["team", "Team", "team_abbr", "bat_team"]:
        if c in df.columns:
            team_col = c
            break
    if team_col is None:
        raise SystemExit("❌ Could not find a team column in bat_today_final.csv for filtering.")

    before_ct = len(df)
    df = df[df[team_col].isin(today_teams)].copy()
    after_ct = len(df)
    print(f"✅ Filtered to today's slate by team: {after_ct}/{before_ct} rows remain.")
    # --------------------------------------------------------------------------

    # Compute projections (adds AB, proj_hits, proj_hr, proj_slg, etc.)
    df_proj = calculate_all_projections(df)

    # Add upstream probabilities for common betting lines so downstream isn’t forced to guess
    # λ for hits is proj_hits; HR is proj_hr; TB approx λ ≈ proj_slg * AB
    for col in ("proj_hits", "proj_hr", "proj_slg", "AB"):
        if col not in df_proj.columns:
            df_proj[col] = float("nan")

    # Hits over 1.5 (i.e., 2+ hits)
    df_proj["prob_hits_over_1p5"] = [
        _poisson_over_prob(lam, 1.5) if pd.notna(lam) else float("nan")
        for lam in pd.to_numeric(df_proj["proj_hits"], errors="coerce")
    ]

    # HR over 0.5 (i.e., 1+ HR)
    df_proj["prob_hr_over_0p5"] = [
        _poisson_over_prob(lam, 0.5) if pd.notna(lam) else float("nan")
        for lam in pd.to_numeric(df_proj["proj_hr"], errors="coerce")
    ]

    # TB over 1.5 (≈ 2+ total bases) using λ ≈ proj_slg * AB
    tb_lambda = pd.to_numeric(df_proj["proj_slg"], errors="coerce") * pd.to_numeric(df_proj["AB"], errors="coerce")
    df_proj["prob_tb_over_1p5"] = [
        _poisson_over_prob(lam, 1.5) if pd.notna(lam) else float("nan")
        for lam in tb_lambda
    ]

    # Sanity: keep probabilities within [0,1] without arbitrary ceilings
    for c in ("prob_hits_over_1p5", "prob_hr_over_0p5", "prob_tb_over_1p5"):
        df_proj[c] = pd.to_numeric(df_proj[c], errors="coerce").clip(0, 1)

    # Persist projections + upstream probabilities
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_proj.to_csv(OUTPUT_FILE, index=False)
    print(
        f"Wrote: {OUTPUT_FILE} (rows={len(df_proj)}) | "
        f"cols={len(df_proj.columns)} incl: prob_hits_over_1p5, prob_hr_over_0p5, prob_tb_over_1p5"
    )

if __name__ == "__main__":
    main()
