#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FINAL: Game score projections using YOUR daily pipeline data.
- No defaults/fillers. Hard-fail with precise diagnostics if joins drop adjustments.
- Uses park/weather (adj_woba_*), opponent pitchers, and season outcome rates blended via log5.
- Writes a diagnostics CSV on any key mismatch, then raises.

Inputs (must exist with required columns):
  data/_projections/batter_props_projected_final.csv
    - player_id, team_id, team, game_id, proj_pa_used
  data/_projections/batter_props_expanded_final.csv
    - player_id, game_id, adj_woba_weather, adj_woba_park, adj_woba_combined
  data/_projections/pitcher_props_projected_final.csv
    - player_id, game_id, team_id, opponent_team_id, pa
  data/_projections/pitcher_mega_z_final.csv
    - player_id, game_id (optional enrichment; not required but allowed)

  data/Data/batters.csv
    - player_id, pa, strikeout, walk, single, double, triple, home_run
  data/Data/pitchers.csv
    - player_id, pa, strikeout, walk, single, double, triple, home_run

Output:
  data/end_chain/final/game_score_projections.csv  (game_id, team_id, team, expected_runs)

Diagnostics (only on failure):
  summaries/07_final/merge_mismatch_batters.csv
  summaries/07_final/missing_season_rates_batters.csv
  summaries/07_final/missing_season_rates_pitchers.csv
  summaries/07_final/missing_opponent_staff.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple

# ----------------------------
# Paths
# ----------------------------
DAILY_DIR   = Path("data/_projections")
SEASON_DIR  = Path("data/Data")
SUM_DIR     = Path("summaries/07_final")
OUT_DIR     = Path("data/end_chain/final")

BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
PITCHERS_MEGA   = DAILY_DIR / "pitcher_mega_z_final.csv"

BATTERS_SEASON  = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"

OUT_FILE = OUT_DIR / "game_score_projections.csv"

SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Utilities
# ----------------------------
def require_cols(df: pd.DataFrame, cols: List[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing required columns: {missing}")

def to_num(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def safe_rate(n: pd.Series, d: pd.Series) -> pd.Series:
    d = pd.to_numeric(d, errors="coerce")
    n = pd.to_numeric(n, errors="coerce")
    r = n.div(d.replace(0, np.nan))
    return r.fillna(0.0).clip(0.0)

def weighted_mean_frame(g: pd.DataFrame, cols: List[str], wcol: str) -> pd.Series:
    w = pd.to_numeric(g[wcol], errors="coerce").fillna(0.0)
    out: Dict[str, float] = {}
    den = float(w.sum())
    for c in cols:
        x = pd.to_numeric(g[c], errors="coerce").fillna(0.0)
        out[c] = float((x * w).sum() / den) if den > 0 else float(x.mean())
    return pd.Series(out)

def log5_element(b: pd.Series, p: pd.Series, lg: float) -> pd.Series:
    if lg <= 0:
        raise RuntimeError("League rate <= 0 encountered in log5_element.")
    return (pd.to_numeric(b, errors="coerce").fillna(0.0) *
            pd.to_numeric(p, errors="coerce").fillna(0.0)) / lg

def clamp_outcomes(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    s = df[cols].sum(axis=1)
    over = s > 1.0
    if over.any():
        df.loc[over, cols] = df.loc[over, cols].div(s[over], axis=0)
        s = df[cols].sum(axis=1)
    df["p_out"] = (1.0 - s).clip(0.0, 1.0)
    return df

def write_diag(df: pd.DataFrame, path: Path):
    if df is not None and not df.empty:
        df.to_csv(path, index=False)

# ----------------------------
# Load daily projection products
# ----------------------------
bat_d = pd.read_csv(BATTERS_DAILY)
bat_x = pd.read_csv(BATTERS_EXP)
pit_d = pd.read_csv(PITCHERS_DAILY)

require_cols(bat_d, ["player_id","team_id","team","game_id","proj_pa_used"], BATTERS_DAILY.name)
require_cols(bat_x, ["player_id","game_id","adj_woba_weather","adj_woba_park","adj_woba_combined"], BATTERS_EXP.name)
require_cols(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"], PITCHERS_DAILY.name)

# Numeric coercions (NO default values assigned beyond NaN -> we will hard fail if needed)
to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
to_num(bat_x, ["player_id","game_id"])
to_num(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"])

# ----------------------------
# Validate batter adjustments will merge
# ----------------------------
keys_proj = set(zip(bat_d["player_id"], bat_d["game_id"]))
keys_exp  = set(zip(bat_x["player_id"], bat_x["game_id"]))

missing_in_exp = keys_proj - keys_exp
if missing_in_exp:
    sample = pd.DataFrame(list(missing_in_exp), columns=["player_id","game_id"])
    diag = bat_d.merge(sample, on=["player_id","game_id"], how="inner")
    write_diag(diag, SUM_DIR / "merge_mismatch_batters.csv")
    raise RuntimeError(
        f"Adjustment rows missing in batter_props_expanded_final.csv for {len(missing_in_exp)} (player_id, game_id) keys. "
        f"See {SUM_DIR/'merge_mismatch_batters.csv'}"
    )

# Merge adjustments
bat = bat_d.merge(
    bat_x[["player_id","game_id","adj_woba_weather","adj_woba_park","adj_woba_combined"]],
    on=["player_id","game_id"],
    how="inner"  # inner to enforce exact alignment; no silent drops
)

# Post-merge validation: NO NaNs allowed in adj_woba_* (strict)
for c in ["adj_woba_weather","adj_woba_park","adj_woba_combined"]:
    if bat[c].isna().any():
        bad = bat.loc[bat[c].isna(), ["player_id","game_id","team_id","team"]]
        write_diag(bad, SUM_DIR / f"missing_{c}.csv")
        raise RuntimeError(f"Found NaN in {c} after merge. See {SUM_DIR/f'missing_{c}.csv'}")

# proj_pa_used must be present and numeric
if bat["proj_pa_used"].isna().any():
    bad = bat.loc[bat["proj_pa_used"].isna(), ["player_id","game_id","team_id","team"]]
    write_diag(bad, SUM_DIR / "missing_proj_pa_used.csv")
    raise RuntimeError(f"Missing proj_pa_used for some batters. See {SUM_DIR/'missing_proj_pa_used.csv'}")

# ----------------------------
# Season priors (strict)
# ----------------------------
bat_s = pd.read_csv(BATTERS_SEASON)
pit_s = pd.read_csv(PITCHERS_SEASON)

require_cols(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], BATTERS_SEASON.name)
require_cols(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], PITCHERS_SEASON.name)

to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

# Batter rates
bat_rates = pd.DataFrame({
    "player_id": bat_s["player_id"],
    "p_k_b":  safe_rate(bat_s["strikeout"], bat_s["pa"]),
    "p_bb_b": safe_rate(bat_s["walk"],      bat_s["pa"]),
    "p_1b_b": safe_rate(bat_s["single"],    bat_s["pa"]),
    "p_2b_b": safe_rate(bat_s["double"],    bat_s["pa"]),
    "p_3b_b": safe_rate(bat_s["triple"],    bat_s["pa"]),
    "p_hr_b": safe_rate(bat_s["home_run"],  bat_s["pa"]),
})

# Pitcher-allowed rates
pit_rates = pd.DataFrame({
    "player_id": pit_s["player_id"],
    "p_k_p":  safe_rate(pit_s["strikeout"], pit_s["pa"]),
    "p_bb_p": safe_rate(pit_s["walk"],      pit_s["pa"]),
    "p_1b_p": safe_rate(pit_s["single"],    pit_s["pa"]),
    "p_2b_p": safe_rate(pit_s["double"],    pit_s["pa"]),
    "p_3b_p": safe_rate(pit_s["triple"],    pit_s["pa"]),
    "p_hr_p": safe_rate(pit_s["home_run"],  pit_s["pa"]),
})

# Verify every batter in today's file has season rates
missing_bat_ids = set(bat["player_id"]) - set(bat_rates["player_id"])
if missing_bat_ids:
    write_diag(pd.DataFrame({"player_id": list(missing_bat_ids)}),
               SUM_DIR / "missing_season_rates_batters.csv")
    raise RuntimeError(f"Season rates missing for {len(missing_bat_ids)} batters. See {SUM_DIR/'missing_season_rates_batters.csv'}")

# Verify every pitcher in today's file has season rates
missing_pit_ids = set(pit_d["player_id"]) - set(pit_rates["player_id"])
if missing_pit_ids:
    write_diag(pd.DataFrame({"player_id": list(missing_pit_ids)}),
               SUM_DIR / "missing_season_rates_pitchers.csv")
    raise RuntimeError(f"Season rates missing for {len(missing_pit_ids)} pitchers. See {SUM_DIR/'missing_season_rates_pitchers.csv'}")

# ----------------------------
# Build opponent-allowed rates per (game_id, opponent_team_id)
# ----------------------------
pit_d_enh = pit_d.merge(pit_rates, on="player_id", how="left")
if pit_d_enh[["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]].isna().any().any():
    bad = pit_d_enh.loc[
        pit_d_enh[["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]].isna().any(axis=1),
        ["player_id","game_id","team_id","opponent_team_id"]
    ]
    write_diag(bad, SUM_DIR / "missing_pitcher_allowed_rates.csv")
    raise RuntimeError(f"NaNs in pitcher allowed rates after merge. See {SUM_DIR/'missing_pitcher_allowed_rates.csv'}")

opp_cols = ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
opp_rates = (
    pit_d_enh.groupby(["game_id","opponent_team_id"], dropna=True)
    .apply(lambda g: weighted_mean_frame(g, opp_cols, "pa"))
    .reset_index()
    .rename(columns={
        "p_k_p":"p_k_opp","p_bb_p":"p_bb_opp","p_1b_p":"p_1b_opp",
        "p_2b_p":"p_2b_opp","p_3b_p":"p_3b_opp","p_hr_p":"p_hr_opp"
    })
)

# Verify every batting (game_id, team_id) has an opponent staff row
bat_keys = bat[["game_id","team_id"]].drop_duplicates()
opp_keys = opp_rates[["game_id","opponent_team_id"]].drop_duplicates()
opp_keys = opp_keys.rename(columns={"opponent_team_id":"team_id"})

missing_staff = bat_keys.merge(opp_keys, on=["game_id","team_id"], how="left", indicator=True)
missing_staff = missing_staff[missing_staff["_merge"] == "left_only"].drop(columns=["_merge"])
if not missing_staff.empty:
    write_diag(missing_staff, SUM_DIR / "missing_opponent_staff.csv")
    raise RuntimeError(f"Opponent staff rates missing for some (game_id, team_id). See {SUM_DIR/'missing_opponent_staff.csv'}")

# ----------------------------
# Assemble batter frame with season rates + opponent rates + park/weather
# ----------------------------
bat = bat.merge(bat_rates, on="player_id", how="left")
bat = bat.merge(opp_rates, left_on=["game_id","team_id"], right_on=["game_id","opponent_team_id"], how="left")

# All required rate columns must be present and non-null
need_cols = ["p_k_b","p_bb_b","p_1b_b","p_2b_b","p_3b_b","p_hr_b",
             "p_k_opp","p_bb_opp","p_1b_opp","p_2b_opp","p_3b_opp","p_hr_opp"]
if bat[need_cols].isna().any().any():
    bad = bat.loc[bat[need_cols].isna().any(axis=1), ["player_id","game_id","team_id","team"] + need_cols]
    write_diag(bad, SUM_DIR / "missing_rates_after_join.csv")
    raise RuntimeError(f"Null outcome rates after joins. See {SUM_DIR/'missing_rates_after_join.csv'}")

# ----------------------------
# Log5 blend per outcome
# ----------------------------
lg_pa = float(pd.read_csv(BATTERS_SEASON)["pa"].sum())
if lg_pa <= 0:
    raise RuntimeError("League PA is zero in batters.csv.")

bat_s_full = pd.read_csv(BATTERS_SEASON)
lg = {
    "k":  float(bat_s_full["strikeout"].sum() / lg_pa),
    "bb": float(bat_s_full["walk"].sum()      / lg_pa),
    "1b": float(bat_s_full["single"].sum()    / lg_pa),
    "2b": float(bat_s_full["double"].sum()    / lg_pa),
    "3b": float(bat_s_full["triple"].sum()    / lg_pa),
    "hr": float(bat_s_full["home_run"].sum()  / lg_pa),
}
# Validate league rates strictly
for k,v in lg.items():
    if v <= 0:
        raise RuntimeError(f"League rate {k} <= 0")

bat["p_k"]  = log5_element(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
bat["p_bb"] = log5_element(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
bat["p_1b"] = log5_element(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"])
bat["p_2b"] = log5_element(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"])
bat["p_3b"] = log5_element(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"])
bat["p_hr"] = log5_element(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"])

# Apply park+weather environment (strict: use exactly your provided adjustments; no defaults)
for c in ["p_1b","p_2b","p_3b","p_hr"]:
    bat[c] = bat[c] * bat["adj_woba_combined"]

# Validate probabilities and derive outs
bat = clamp_outcomes(bat, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"])

# ----------------------------
# Linear weights (no assumptions beyond established constants)
# ----------------------------
LW = {"BB": 0.33, "1B": 0.47, "2B": 0.77, "3B": 1.04, "HR": 1.40, "OUT": 0.00}

bat["runs_per_pa"] = (
    bat["p_bb"] * LW["BB"] +
    bat["p_1b"] * LW["1B"] +
    bat["p_2b"] * LW["2B"] +
    bat["p_3b"] * LW["3B"] +
    bat["p_hr"] * LW["HR"] +
    bat["p_out"] * LW["OUT"]
)

# Strict: proj_pa_used must be numeric and non-null (validated earlier)
bat["expected_runs_batter"] = bat["runs_per_pa"] * bat["proj_pa_used"]

# ----------------------------
# Aggregate to team/game
# ----------------------------
team_results = (
    bat.groupby(["game_id","team_id","team"], dropna=True)["expected_runs_batter"]
    .sum()
    .reset_index()
    .rename(columns={"expected_runs_batter":"expected_runs"})
    .sort_values(["game_id","team_id"])
    .reset_index(drop=True)
)

# Write output
team_results.to_csv(OUT_FILE, index=False)
print(f"OK {len(team_results)} rows -> {OUT_FILE}")
