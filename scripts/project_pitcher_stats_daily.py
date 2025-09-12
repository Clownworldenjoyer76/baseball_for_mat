#!/usr/bin/env python3
# Purpose:
#   Produce daily pitcher opponent-adjusted event probabilities + expected runs allowed.
#   Uses opponent LINEUP (from batter_props_projected_final.csv) and season priors (batters & pitchers).
#   Applies environment via lineup-weighted adj_woba_combined.
# Inputs:
#   data/_projections/pitcher_props_projected_final.csv  (player_id, game_id, team_id, opponent_team_id, pa)
#   data/_projections/batter_props_projected_final.csv   (player_id, team_id, team, game_id, proj_pa_used)
#   data/_projections/batter_props_expanded_final.csv    (player_id, game_id, adj_woba_*)
#   data/Data/pitchers.csv, data/Data/batters.csv        (season totals)
# Outputs:
#   data/_projections/pitcher_event_probs_daily.csv
#   data/end_chain/final/pitcher_event_probs_daily.csv
# Notes:
#   expected_runs_allowed is computed vs opponent team lineup: runs_per_pa_vs_lineup * sum(proj_pa_used for that lineup).

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")

PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"

OUT_FILE_PROJ  = DAILY_DIR / "pitcher_event_probs_daily.csv"
OUT_FILE_FINAL = OUT_DIR   / "pitcher_event_probs_daily.csv"

ADJ_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

def write_text(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8")

def require(df: pd.DataFrame, cols: list[str], name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing columns: {miss}")

def to_num(df: pd.DataFrame, cols: list[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def safe_rate(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce").replace(0, np.nan)
    return (n / d).fillna(0.0).clip(0.0)

def log5(b, p, lg):
    b = pd.to_numeric(b, errors="coerce").fillna(0.0)
    p = pd.to_numeric(p, errors="coerce").fillna(0.0)
    lg = float(lg)
    if lg <= 0:
        raise RuntimeError("League rate <= 0")
    return (b * p) / lg

def weighted_mean(series, weights):
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    den = float(w.sum())
    x = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return float((x * w).sum() / den) if den > 0 else float(x.mean())

def main():
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("LOAD inputs")
    pit_d = pd.read_csv(PITCHERS_DAILY)
    bat_d = pd.read_csv(BATTERS_DAILY)
    bat_x = pd.read_csv(BATTERS_EXP)
    pit_s = pd.read_csv(PITCHERS_SEASON)
    bat_s = pd.read_csv(BATTERS_SEASON)

    require(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"], str(PITCHERS_DAILY))
    require(bat_d, ["player_id","team_id","game_id","proj_pa_used"], str(BATTERS_DAILY))
    require(bat_x, ["player_id","game_id"] + ADJ_COLS, str(BATTERS_EXP))
    require(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(PITCHERS_SEASON))
    require(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(BATTERS_SEASON))

    to_num(pit_d, ["player_id","team_id","opponent_team_id","game_id","pa"])
    to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
    to_num(bat_x, ["player_id","game_id"])
    to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])
    to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])

    # Season priors
    pit_rates = pd.DataFrame({
        "player_id": pit_s["player_id"],
        "p_k_p":  safe_rate(pit_s["strikeout"], pit_s["pa"]),
        "p_bb_p": safe_rate(pit_s["walk"],      pit_s["pa"]),
        "p_1b_p": safe_rate(pit_s["single"],    pit_s["pa"]),
        "p_2b_p": safe_rate(pit_s["double"],    pit_s["pa"]),
        "p_3b_p": safe_rate(pit_s["triple"],    pit_s["pa"]),
        "p_hr_p": safe_rate(pit_s["home_run"],  pit_s["pa"]),
    })

    bat_rates = pd.DataFrame({
        "player_id": bat_s["player_id"],
        "p_k_b":  safe_rate(bat_s["strikeout"], bat_s["pa"]),
        "p_bb_b": safe_rate(bat_s["walk"],      bat_s["pa"]),
        "p_1b_b": safe_rate(bat_s["single"],    bat_s["pa"]),
        "p_2b_b": safe_rate(bat_s["double"],    bat_s["pa"]),
        "p_3b_b": safe_rate(bat_s["triple"],    bat_s["pa"]),
        "p_hr_b": safe_rate(bat_s["home_run"],  bat_s["pa"]),
    })

    # League rates from season (for log5)
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa),
        "bb": float(bat_s["walk"].sum()      / lg_pa),
        "1b": float(bat_s["single"].sum()    / lg_pa),
        "2b": float(bat_s["double"].sum()    / lg_pa),
        "3b": float(bat_s["triple"].sum()    / lg_pa),
        "hr": float(bat_s["home_run"].sum()  / lg_pa),
    }

    # Build lineup table with season batter priors and env factors
    lineup = (bat_d
              .merge(bat_rates, on="player_id", how="left")
              .merge(bat_x[["player_id","game_id","adj_woba_combined"]], on=["player_id","game_id"], how="left"))
    need = ["p_k_b","p_bb_b","p_1b_b","p_2b_b","p_3b_b","p_hr_b","adj_woba_combined"]
    if lineup[need].isna().any().any():
        lineup.loc[lineup[need].isna().any(axis=1)].to_csv(SUM_DIR / "missing_lineup_rates_env.csv", index=False)
        raise RuntimeError("Null lineup rates/env; see summaries/07_final/missing_lineup_rates_env.csv")

    # Aggregate opponent lineup by (game_id, team_id) weights=proj_pa_used
    def agg_team(g: pd.DataFrame) -> pd.Series:
        w = g["proj_pa_used"]
        return pd.Series({
            "team_proj_pa": float(pd.to_numeric(w, errors="coerce").fillna(0.0).sum()),
            "p_k_b_team":  weighted_mean(g["p_k_b"],  w),
            "p_bb_b_team": weighted_mean(g["p_bb_b"], w),
            "p_1b_b_team": weighted_mean(g["p_1b_b"], w),
            "p_2b_b_team": weighted_mean(g["p_2b_b"], w),
            "p_3b_b_team": weighted_mean(g["p_3b_b"], w),
            "p_hr_b_team": weighted_mean(g["p_hr_b"], w),
            "env_adj_team": weighted_mean(g["adj_woba_combined"], w),
        })

    team_bat = (lineup.groupby(["game_id","team_id"], as_index=False)
                .apply(agg_team, include_groups=False)
                .reset_index())

    # Attach pitcher priors
    pit = pit_d.merge(pit_rates, on="player_id", how="left")
    if pit[["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]].isna().any().any():
        pit.loc[pit[["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]].isna().any(axis=1)].to_csv(
            SUM_DIR / "missing_pitcher_priors.csv", index=False
        )
        raise RuntimeError("Missing pitcher season priors; see summaries/07_final/missing_pitcher_priors.csv")

    # For each pitcher row (game_id, opponent_team_id) join opponent lineup aggregate at (game_id, team_id=opponent_team_id)
    pit = pit.merge(
        team_bat.rename(columns={"team_id":"opponent_team_id"}),  # (game_id, opponent_team_id)
        on=["game_id","opponent_team_id"], how="left"
    )

    need_team = ["team_proj_pa","p_k_b_team","p_bb_b_team","p_1b_b_team","p_2b_b_team","p_3b_b_team","p_hr_b_team","env_adj_team"]
    if pit[need_team].isna().any().any():
        pit.loc[pit[need_team].isna().any(axis=1),
                ["player_id","game_id","team_id","opponent_team_id"]+need_team].to_csv(
            SUM_DIR / "missing_opponent_lineup_for_pitcher.csv", index=False
        )
        raise RuntimeError("Missing opponent lineup aggregate; see summaries/07_final/missing_opponent_lineup_for_pitcher.csv")

    # log5 pitcher vs opponent lineup, then apply team environment to hit types
    pit["p_k_vs"]  = log5(pit["p_k_b_team"],  pit["p_k_p"],  lg["k"])
    pit["p_bb_vs"] = log5(pit["p_bb_b_team"], pit["p_bb_p"], lg["bb"])
    pit["p_1b_vs"] = log5(pit["p_1b_b_team"], pit["p_1b_p"], lg["1b"]) * pit["env_adj_team"]
    pit["p_2b_vs"] = log5(pit["p_2b_b_team"], pit["p_2b_p"], lg["2b"]) * pit["env_adj_team"]
    pit["p_3b_vs"] = log5(pit["p_3b_b_team"], pit["p_3b_p"], lg["3b"]) * pit["env_adj_team"]
    pit["p_hr_vs"] = log5(pit["p_hr_b_team"], pit["p_hr_p"], lg["hr"]) * pit["env_adj_team"]

    for c in ["p_k_vs","p_bb_vs","p_1b_vs","p_2b_vs","p_3b_vs","p_hr_vs"]:
        pit[c] = pd.to_numeric(pit[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    s = pit[["p_k_vs","p_bb_vs","p_1b_vs","p_2b_vs","p_3b_vs","p_hr_vs"]].sum(axis=1)
    over = s > 1.0
    if over.any():
        pit.loc[over, ["p_k_vs","p_bb_vs","p_1b_vs","p_2b_vs","p_3b_vs","p_hr_vs"]] = \
            pit.loc[over, ["p_k_vs","p_bb_vs","p_1b_vs","p_2b_vs","p_3b_vs","p_hr_vs"]].div(s[over], axis=0)
    s = pit[["p_k_vs","p_bb_vs","p_1b_vs","p_2b_vs","p_3b_vs","p_hr_vs"]].sum(axis=1)
    pit["p_out_vs"] = (1.0 - s).clip(0.0, 1.0)

    # Linear-weights — expected runs allowed vs opponent lineup PA
    LW = {"BB":0.33,"1B":0.47,"2B":0.77,"3B":1.04,"HR":1.40,"OUT":0.0}
    pit["runs_per_pa_allowed"] = (
        pit["p_bb_vs"]*LW["BB"] + pit["p_1b_vs"]*LW["1B"] + pit["p_2b_vs"]*LW["2B"] +
        pit["p_3b_vs"]*LW["3B"] + pit["p_hr_vs"]*LW["HR"] + pit["p_out_vs"]*LW["OUT"]
    )
    pit["expected_runs_allowed"] = pit["runs_per_pa_allowed"] * pit["team_proj_pa"]

    keep = [
        "player_id","game_id","team_id","opponent_team_id","pa","team_proj_pa","env_adj_team",
        "p_k_vs","p_bb_vs","p_1b_vs","p_2b_vs","p_3b_vs","p_hr_vs","p_out_vs",
        "runs_per_pa_allowed","expected_runs_allowed"
    ]
    out = pit[keep].copy().sort_values(["game_id","team_id","player_id"]).reset_index(drop=True)

    OUT_FILE_PROJ.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE_FINAL.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE_PROJ, index=False)
    out.to_csv(OUT_FILE_FINAL, index=False)

    write_text(SUM_DIR / "pitcher_event_probs_status.txt", f"OK pitcher_event_probs_daily rows={len(out)}")
    print(f"WROTE: {len(out)} rows -> {OUT_FILE_PROJ} AND {OUT_FILE_FINAL}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        write_text(SUM_DIR / "pitcher_event_probs_status.txt", "FAIL pitcher_event_probs_daily")
        write_text(SUM_DIR / "pitcher_event_probs_errors.txt", repr(e))
        raise
