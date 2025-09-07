#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data-driven team run projections using your daily projections (park/weather/opponent aware)
plus season outcome rates with log5 blending and linear weights. No hard-coded
"target runs"; outputs come from your data.

Inputs (produced by your pipeline):
  - data/_projections/batter_props_projected_final.csv   (player_id, team_id, team, game_id, proj_pa_used, *…*)
  - data/_projections/batter_props_expanded_final.csv    (player_id, game_id, adj_woba_weather, adj_woba_park, adj_woba_combined, *…*)
  - data/_projections/pitcher_props_projected_final.csv  (player_id, game_id, team_id, opponent_team_id, pa, *…*)
  - data/_projections/pitcher_mega_z_final.csv           (player_id, game_id, many pitcher features)

Season priors (full stat matrices you provided):
  - data/Data/batters.csv   (player_id, pa, strikeout, walk, single, double, triple, home_run, *…*)
  - data/Data/pitchers.csv  (player_id, pa, strikeout, walk, single, double, triple, home_run, *…*)

Output:
  - data/end_chain/final/game_score_projections.csv  (game_id, team_id, team, expected_runs)
"""

from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Dict

# ----------------------------
# Paths
# ----------------------------
DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
OUT_DIR = Path("data/end_chain/final")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_DAILY = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP   = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY = DAILY_DIR / "pitcher_props_projected_final.csv"
PITCHERS_MEGA  = DAILY_DIR / "pitcher_mega_z_final.csv"

BATTERS_SEASON = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"

OUT_FILE = OUT_DIR / "game_score_projections.csv"


# ----------------------------
# Utilities
# ----------------------------
def require_cols(df: pd.DataFrame, cols: List[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing required columns: {missing}")

def safe_rate(n: pd.Series, d: pd.Series) -> pd.Series:
    d = pd.to_numeric(d, errors="coerce")
    n = pd.to_numeric(n, errors="coerce")
    r = n.div(d.replace(0, np.nan))
    return r.fillna(0.0).clip(0.0)

def to_num(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def weighted_mean_frame(g: pd.DataFrame, cols: List[str], wcol: str) -> pd.Series:
    w = pd.to_numeric(g[wcol], errors="coerce").fillna(0.0)
    out: Dict[str, float] = {}
    den = w.sum()
    if den <= 0:
        for c in cols:
            out[c] = float(pd.to_numeric(g[c], errors="coerce").mean())
        return pd.Series(out)
    for c in cols:
        x = pd.to_numeric(g[c], errors="coerce").fillna(0.0)
        out[c] = float((x * w).sum() / den)
    return pd.Series(out)

def log5_element(b: pd.Series, p: pd.Series, lg: float) -> pd.Series:
    # (Batter * Pitcher) / League ; guard zero league rate
    if lg <= 0:
        lg = 1e-12
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


# ----------------------------
# Load daily projection products (your pipeline)
# ----------------------------
bat_d = pd.read_csv(BATTERS_DAILY)
bat_x = pd.read_csv(BATTERS_EXP)
pit_d = pd.read_csv(PITCHERS_DAILY)
pit_m = pd.read_csv(PITCHERS_MEGA)

require_cols(bat_d, ["player_id","team_id","team","game_id","proj_pa_used"], BATTERS_DAILY.name)
require_cols(bat_x, ["player_id","game_id"], BATTERS_EXP.name)
require_cols(pit_d, ["player_id","game_id","team_id","opponent_team_id"], PITCHERS_DAILY.name)

# numeric coercions
to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
to_num(bat_x, ["player_id","game_id","adj_woba_weather","adj_woba_park","adj_woba_combined"])
to_num(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"])

# merge park/weather adjustments into batters
adj_cols = ["adj_woba_weather","adj_woba_park","adj_woba_combined"]
for c in adj_cols:
    if c not in bat_x.columns:
        bat_x[c] = np.nan

bat = bat_d.merge(
    bat_x[["player_id","game_id"] + adj_cols],
    on=["player_id","game_id"],
    how="left"
)

# defaults if missing
bat["adj_woba_weather"]  = pd.to_numeric(bat["adj_woba_weather"], errors="coerce").fillna(1.0)
bat["adj_woba_park"]     = pd.to_numeric(bat["adj_woba_park"], errors="coerce").fillna(1.0)
bat["adj_woba_combined"] = pd.to_numeric(bat["adj_woba_combined"], errors="coerce")
bat["adj_woba_combined"] = bat["adj_woba_combined"].fillna(
    (bat["adj_woba_weather"] + bat["adj_woba_park"]) / 2.0
)

bat["proj_pa_used"] = pd.to_numeric(bat["proj_pa_used"], errors="coerce").fillna(0.0).clip(lower=0.0)

# attach mega pitcher features (kept for richness; not required for core math)
to_num(pit_m, ["player_id","game_id"])
pit_d_full = pit_d.merge(pit_m, on=["player_id","game_id"], how="left")


# ----------------------------
# Load season priors and compute outcome rates
# ----------------------------
bat_s = pd.read_csv(BATTERS_SEASON)
pit_s = pd.read_csv(PITCHERS_SEASON)

require_cols(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], BATTERS_SEASON.name)
require_cols(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], PITCHERS_SEASON.name)

to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

# batter rates
bat_rates = pd.DataFrame({
    "player_id": bat_s["player_id"],
    "p_k_b":  safe_rate(bat_s["strikeout"], bat_s["pa"]),
    "p_bb_b": safe_rate(bat_s["walk"],     bat_s["pa"]),
    "p_1b_b": safe_rate(bat_s["single"],   bat_s["pa"]),
    "p_2b_b": safe_rate(bat_s["double"],   bat_s["pa"]),
    "p_3b_b": safe_rate(bat_s["triple"],   bat_s["pa"]),
    "p_hr_b": safe_rate(bat_s["home_run"], bat_s["pa"]),
})

# pitcher-allowed rates
pit_rates = pd.DataFrame({
    "player_id": pit_s["player_id"],
    "p_k_p":  safe_rate(pit_s["strikeout"], pit_s["pa"]),
    "p_bb_p": safe_rate(pit_s["walk"],      pit_s["pa"]),
    "p_1b_p": safe_rate(pit_s["single"],    pit_s["pa"]),
    "p_2b_p": safe_rate(pit_s["double"],    pit_s["pa"]),
    "p_3b_p": safe_rate(pit_s["triple"],    pit_s["pa"]),
    "p_hr_p": safe_rate(pit_s["home_run"],  pit_s["pa"]),
})

# league base rates (from batter season totals)
lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
if lg_pa <= 0:
    raise RuntimeError("League PA is zero; cannot compute league rates.")
lg = {
    "k":  float(pd.to_numeric(bat_s["strikeout"], errors="coerce").sum() / lg_pa),
    "bb": float(pd.to_numeric(bat_s["walk"],      errors="coerce").sum() / lg_pa),
    "1b": float(pd.to_numeric(bat_s["single"],    errors="coerce").sum() / lg_pa),
    "2b": float(pd.to_numeric(bat_s["double"],    errors="coerce").sum() / lg_pa),
    "3b": float(pd.to_numeric(bat_s["triple"],    errors="coerce").sum() / lg_pa),
    "hr": float(pd.to_numeric(bat_s["home_run"],  errors="coerce").sum() / lg_pa),
}

# ----------------------------
# Build opponent-allowed rates per (game_id, opponent_team_id)
# weight by daily PA across all pitchers expected to face that opponent
# ----------------------------
pit_d_enh = pit_d_full.merge(pit_rates, on="player_id", how="left")
to_num(pit_d_enh, ["pa"])
for c in ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]:
    pit_d_enh[c] = pd.to_numeric(pit_d_enh[c], errors="coerce").fillna(lg[c.split('_')[1]])

opp_cols = ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
opp_rates = (
    pit_d_enh.groupby(["game_id","opponent_team_id"], dropna=True)
    .apply(lambda g: weighted_mean_frame(g, opp_cols, "pa" if "pa" in g.columns else opp_cols[0]))
    .reset_index()
    .rename(columns={
        "p_k_p":"p_k_opp","p_bb_p":"p_bb_opp","p_1b_p":"p_1b_opp",
        "p_2b_p":"p_2b_opp","p_3b_p":"p_3b_opp","p_hr_p":"p_hr_opp"
    })
)

# ----------------------------
# Assemble batter frame with batter season rates + opponent rates + park/weather
# ----------------------------
bat = bat.merge(bat_rates, on="player_id", how="left")
bat = bat.merge(opp_rates, left_on=["game_id","team_id"], right_on=["game_id","opponent_team_id"], how="left")

# fill any missing with league rates to avoid dropping a team
fill_pairs = [
    ("p_k_b","k"),("p_bb_b","bb"),("p_1b_b","1b"),("p_2b_b","2b"),("p_3b_b","3b"),("p_hr_b","hr"),
    ("p_k_opp","k"),("p_bb_opp","bb"),("p_1b_opp","1b"),("p_2b_opp","2b"),("p_3b_opp","3b"),("p_hr_opp","hr"),
]
for col, key in fill_pairs:
    bat[col] = pd.to_numeric(bat[col], errors="coerce").fillna(lg[key])

# ----------------------------
# Log5 blend per outcome for each batter vs opponent staff
# ----------------------------
bat["p_k"]  = log5_element(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
bat["p_bb"] = log5_element(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
bat["p_1b"] = log5_element(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"])
bat["p_2b"] = log5_element(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"])
bat["p_3b"] = log5_element(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"])
bat["p_hr"] = log5_element(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"])

# Apply park+weather scaling to hit outcomes (not walks)
# Using your adj_woba_combined as multiplicative environment effect
for c in ["p_1b","p_2b","p_3b","p_hr"]:
    bat[c] = (bat[c] * bat["adj_woba_combined"]).clip(0.0, 1.0)

# Ensure probabilities are valid and derive outs
bat = clamp_outcomes(bat, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"])

# ----------------------------
# Linear weights (run value per PA by outcome)
# ----------------------------
LW = {
    "BB": 0.33,
    "1B": 0.47,
    "2B": 0.77,
    "3B": 1.04,
    "HR": 1.40,
    "OUT": 0.00,
}

bat["runs_per_pa"] = (
    bat["p_bb"] * LW["BB"] +
    bat["p_1b"] * LW["1B"] +
    bat["p_2b"] * LW["2B"] +
    bat["p_3b"] * LW["3B"] +
    bat["p_hr"] * LW["HR"] +
    bat["p_out"] * LW["OUT"]
)

bat["expected_runs_batter"] = bat["runs_per_pa"] * bat["proj_pa_used"]

# ----------------------------
# Aggregate to team/game (guarantees two rows per game when inputs complete)
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
