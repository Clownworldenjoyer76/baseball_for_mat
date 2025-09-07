#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Game score projections using YOUR pipeline outputs.
Strict: no silent defaults; fail with diagnostics if joins drop adjustments.

Outputs:
  data/end_chain/final/game_score_projections.csv  (game_id, team_id, team, expected_runs)
"""

from pathlib import Path
import pandas as pd
import numpy as np

DAILY_DIR   = Path("data/_projections")
SEASON_DIR  = Path("data/Data")
SUM_DIR     = Path("summaries/07_final")
OUT_DIR     = Path("data/end_chain/final")
SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"

OUT_FILE = OUT_DIR / "game_score_projections.csv"

def require(df, cols, name):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing required columns: {miss}")

def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def safe_rate(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce").replace(0, np.nan)
    return (n / d).fillna(0.0).clip(0.0)

def weighted_mean(g, cols, wcol):
    w = pd.to_numeric(g[wcol], errors="coerce").fillna(0.0)
    den = float(w.sum())
    out = {}
    for c in cols:
        x = pd.to_numeric(g[c], errors="coerce").fillna(0.0)
        out[c] = float((x * w).sum() / den) if den > 0 else float(x.mean())
    return pd.Series(out)

def log5(b, p, lg):
    if lg <= 0:
        raise RuntimeError("League rate <= 0")
    return (pd.to_numeric(b, errors="coerce").fillna(0.0) *
            pd.to_numeric(p, errors="coerce").fillna(0.0)) / lg

# Load
bat_d = pd.read_csv(BATTERS_DAILY)
bat_x = pd.read_csv(BATTERS_EXP)
pit_d = pd.read_csv(PITCHERS_DAILY)
bat_s = pd.read_csv(BATTERS_SEASON)
pit_s = pd.read_csv(PITCHERS_SEASON)

require(bat_d, ["player_id","team_id","team","game_id","proj_pa_used"], str(BATTERS_DAILY))
require(bat_x, ["player_id","game_id","adj_woba_weather","adj_woba_park","adj_woba_combined"], str(BATTERS_EXP))
require(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"], str(PITCHERS_DAILY))
require(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(BATTERS_SEASON))
require(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(PITCHERS_SEASON))

to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
to_num(bat_x, ["player_id","game_id"])
to_num(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"])
to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

# Validate key coverage BEFORE merge
keys_proj = set(zip(bat_d["player_id"], bat_d["game_id"]))
keys_exp  = set(zip(bat_x["player_id"], bat_x["game_id"]))
missing_in_exp = keys_proj - keys_exp
if missing_in_exp:
    pd.DataFrame(list(missing_in_exp), columns=["player_id","game_id"]).to_csv(
        SUM_DIR / "merge_mismatch_batters.csv", index=False
    )
    raise RuntimeError(f"adjustments missing for {len(missing_in_exp)} batter (player_id,game_id) keys -> summaries/07_final/merge_mismatch_batters.csv")

# Merge park/weather strictly (inner)
bat = bat_d.merge(
    bat_x[["player_id","game_id","adj_woba_weather","adj_woba_park","adj_woba_combined"]],
    on=["player_id","game_id"],
    how="inner"
)

# Season rates
bat_rates = pd.DataFrame({
    "player_id": bat_s["player_id"],
    "p_k_b":  safe_rate(bat_s["strikeout"], bat_s["pa"]),
    "p_bb_b": safe_rate(bat_s["walk"],      bat_s["pa"]),
    "p_1b_b": safe_rate(bat_s["single"],    bat_s["pa"]),
    "p_2b_b": safe_rate(bat_s["double"],    bat_s["pa"]),
    "p_3b_b": safe_rate(bat_s["triple"],    bat_s["pa"]),
    "p_hr_b": safe_rate(bat_s["home_run"],  bat_s["pa"]),
})
pit_rates = pd.DataFrame({
    "player_id": pit_s["player_id"],
    "p_k_p":  safe_rate(pit_s["strikeout"], pit_s["pa"]),
    "p_bb_p": safe_rate(pit_s["walk"],      pit_s["pa"]),
    "p_1b_p": safe_rate(pit_s["single"],    pit_s["pa"]),
    "p_2b_p": safe_rate(pit_s["double"],    pit_s["pa"]),
    "p_3b_p": safe_rate(pit_s["triple"],    pit_s["pa"]),
    "p_hr_p": safe_rate(pit_s["home_run"],  pit_s["pa"]),
})

# Confirm every batter/pitcher has season rows
miss_b = set(bat["player_id"]) - set(bat_rates["player_id"])
if miss_b:
    pd.DataFrame({"player_id": list(miss_b)}).to_csv(SUM_DIR/"missing_season_rates_batters.csv", index=False)
    raise RuntimeError("season rates missing for some batters -> summaries/07_final/missing_season_rates_batters.csv")
miss_p = set(pit_d["player_id"]) - set(pit_rates["player_id"])
if miss_p:
    pd.DataFrame({"player_id": list(miss_p)}).to_csv(SUM_DIR/"missing_season_rates_pitchers.csv", index=False)
    raise RuntimeError("season rates missing for some pitchers -> summaries/07_final/missing_season_rates_pitchers.csv")

# Opponent staff weighted rates per (game_id, opponent_team_id)
pit_d_enh = pit_d.merge(pit_rates, on="player_id", how="left")
rate_cols = ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
if pit_d_enh[rate_cols].isna().any().any():
    bad = pit_d_enh.loc[pit_d_enh[rate_cols].isna().any(axis=1), ["player_id","game_id","team_id","opponent_team_id"]]
    bad.to_csv(SUM_DIR/"missing_pitcher_allowed_rates.csv", index=False)
    raise RuntimeError("NaNs in pitcher allowed rates -> summaries/07_final/missing_pitcher_allowed_rates.csv")

opp_rates = (
    pit_d_enh.groupby(["game_id","opponent_team_id"], dropna=True)
    .apply(lambda g: weighted_mean(g, rate_cols, "pa"))
    .reset_index()
    .rename(columns={
        "p_k_p":"p_k_opp","p_bb_p":"p_bb_opp","p_1b_p":"p_1b_opp",
        "p_2b_p":"p_2b_opp","p_3b_p":"p_3b_opp","p_hr_p":"p_hr_opp"
    })
)

# Ensure each batting side finds an opponent staff row
want = bat[["game_id","team_id"]].drop_duplicates()
have = opp_rates.rename(columns={"opponent_team_id":"team_id"})[["game_id","team_id"]].drop_duplicates()
anti = want.merge(have, on=["game_id","team_id"], how="left", indicator=True)
anti = anti[anti["_merge"]=="left_only"].drop(columns=["_merge"])
if not anti.empty:
    anti.to_csv(SUM_DIR/"missing_opponent_staff.csv", index=False)
    raise RuntimeError("opponent staff not found for some (game_id,team_id) -> summaries/07_final/missing_opponent_staff.csv")

# Assemble per-batter outcomes
bat = bat.merge(bat_rates, on="player_id", how="left")
bat = bat.merge(opp_rates, left_on=["game_id","team_id"], right_on=["game_id","opponent_team_id"], how="left")

need = ["p_k_b","p_bb_b","p_1b_b","p_2b_b","p_3b_b","p_hr_b","p_k_opp","p_bb_opp","p_1b_opp","p_2b_opp","p_3b_opp","p_hr_opp"]
if bat[need].isna().any().any():
    bat.loc[bat[need].isna().any(axis=1), ["player_id","game_id","team_id","team"]+need].to_csv(
        SUM_DIR/"missing_rates_after_join.csv", index=False
    )
    raise RuntimeError("null outcome rates after join -> summaries/07_final/missing_rates_after_join.csv")

# League averages (from batter season totals)
lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
lg = {
    "k":  float(bat_s["strikeout"].sum() / lg_pa),
    "bb": float(bat_s["walk"].sum()      / lg_pa),
    "1b": float(bat_s["single"].sum()    / lg_pa),
    "2b": float(bat_s["double"].sum()    / lg_pa),
    "3b": float(bat_s["triple"].sum()    / lg_pa),
    "hr": float(bat_s["home_run"].sum()  / lg_pa),
}

bat["p_k"]  = log5(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
bat["p_bb"] = log5(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
bat["p_1b"] = log5(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"])
bat["p_2b"] = log5(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"])
bat["p_3b"] = log5(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"])
bat["p_hr"] = log5(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"])

# Apply YOUR park+weather environment
bat["p_1b"] *= bat["adj_woba_combined"]
bat["p_2b"] *= bat["adj_woba_combined"]
bat["p_3b"] *= bat["adj_woba_combined"]
bat["p_hr"] *= bat["adj_woba_combined"]

# Clamp and outs
for c in ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]:
    bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
over = s > 1.0
if over.any():
    bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]] = bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].div(s[over], axis=0)
    s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
bat["p_out"] = (1.0 - s).clip(0.0, 1.0)

# Linear weights
LW = {"BB":0.33,"1B":0.47,"2B":0.77,"3B":1.04,"HR":1.40,"OUT":0.0}
bat["runs_per_pa"] = (
    bat["p_bb"]*LW["BB"] + bat["p_1b"]*LW["1B"] + bat["p_2b"]*LW["2B"] +
    bat["p_3b"]*LW["3B"] + bat["p_hr"]*LW["HR"] + bat["p_out"]*LW["OUT"]
)

# Expected runs per batter
bat["expected_runs_batter"] = bat["runs_per_pa"] * bat["proj_pa_used"]

# Aggregate to team/game
team = (
    bat.groupby(["game_id","team_id","team"], dropna=True)["expected_runs_batter"]
    .sum().reset_index()
    .rename(columns={"expected_runs_batter":"expected_runs"})
    .sort_values(["game_id","team_id"]).reset_index(drop=True)
)

# Write
team.to_csv(OUT_FILE, index=False)
print(f"OK {len(team)} rows -> {OUT_FILE}")
