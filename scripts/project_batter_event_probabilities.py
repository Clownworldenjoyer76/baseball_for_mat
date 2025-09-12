#!/usr/bin/env python3
# Purpose:
#   Produce daily batter event probabilities with opponent-starter and env (weather+park) baked in.
# Inputs:
#   data/_projections/batter_props_projected_final.csv   (player_id, team_id, team, game_id, proj_pa_used, ...)
#   data/_projections/batter_props_expanded_final.csv    (player_id, game_id, adj_woba_weather, adj_woba_park, adj_woba_combined)
#   data/_projections/pitcher_props_projected_final.csv  (player_id, game_id, team_id, opponent_team_id, pa)
#   data/Data/batters.csv                                 (player_id, pa, strikeout, walk, single, double, triple, home_run)
#   data/Data/pitchers.csv                                (player_id, pa, strikeout, walk, single, double, triple, home_run)
# Outputs:
#   data/_projections/batter_event_probs_daily.csv
#   data/end_chain/final/batter_event_probs_daily.csv
#   summaries/07_final/batter_event_probs_status.txt, errors/log CSVs on failure

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")

BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"

OUT_FILE_PROJ = DAILY_DIR / "batter_event_probs_daily.csv"
OUT_FILE_FINAL = OUT_DIR / "batter_event_probs_daily.csv"

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

def main():
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("LOAD inputs")
    bat_d = pd.read_csv(BATTERS_DAILY)
    bat_x = pd.read_csv(BATTERS_EXP)
    pit_d = pd.read_csv(PITCHERS_DAILY)
    bat_s = pd.read_csv(BATTERS_SEASON)
    pit_s = pd.read_csv(PITCHERS_SEASON)

    require(bat_d, ["player_id","team_id","team","game_id","proj_pa_used"], str(BATTERS_DAILY))
    require(bat_x, ["player_id","game_id"] + ADJ_COLS, str(BATTERS_EXP))
    require(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"], str(PITCHERS_DAILY))
    require(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(BATTERS_SEASON))
    require(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(PITCHERS_SEASON))

    to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
    to_num(bat_x, ["player_id","game_id"])
    to_num(pit_d, ["player_id","team_id","opponent_team_id","game_id","pa"])
    to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
    to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

    # Coverage: each batter game must have an opponent starter in pit_d
    bat_games = set(pd.unique(bat_d["game_id"]))
    pit_games = set(pd.unique(pit_d["game_id"]))
    missing_games = sorted(list(bat_games - pit_games))
    if missing_games:
        pd.DataFrame({"game_id": missing_games}).to_csv(SUM_DIR / "missing_opponent_starter_games.csv", index=False)
        raise RuntimeError("Batter games lacking opponent starter; see summaries/07_final/missing_opponent_starter_games.csv")

    # Keys for adjustment must exist in expanded
    keys_proj = set(zip(bat_d["player_id"], bat_d["game_id"]))
    keys_exp  = set(zip(bat_x["player_id"], bat_x["game_id"]))
    missing = keys_proj - keys_exp
    if missing:
        pd.DataFrame(list(missing), columns=["player_id","game_id"]).to_csv(SUM_DIR / "merge_mismatch_batters.csv", index=False)
        raise RuntimeError("Adjustment keys missing; see summaries/07_final/merge_mismatch_batters.csv")

    # Merge adjustments
    bat = bat_d.drop(columns=[c for c in ADJ_COLS if c in bat_d.columns], errors="ignore") \
               .merge(bat_x[["player_id","game_id"] + ADJ_COLS], on=["player_id","game_id"], how="inner")

    for c in ADJ_COLS:
        if c not in bat.columns or bat[c].isna().any():
            bad = bat.loc[bat[c].isna()] if c in bat.columns else bat[["player_id","game_id"]]
            bad.to_csv(SUM_DIR / f"missing_{c}.csv", index=False)
            raise RuntimeError(f"{c} invalid after merge; see summaries/07_final/missing_{c}.csv")

    # Season priors
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

    # Attach pitcher season rates, pick one opp starter per (game_id, opponent_team_id) by highest pa
    pit_d_enh = pit_d.merge(pit_rates, on="player_id", how="left")
    opp_pick = (pit_d_enh
                .sort_values(["game_id","opponent_team_id","pa"], ascending=[True, True, False])
                .drop_duplicates(["game_id","opponent_team_id"]))
    opp_cols = ["game_id","opponent_team_id","player_id","p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
    opp = opp_pick[opp_cols].rename(columns={
        "opponent_team_id":"team_id",
        "player_id":"opp_pitcher_id",
        "p_k_p":"p_k_opp","p_bb_p":"p_bb_opp","p_1b_p":"p_1b_opp",
        "p_2b_p":"p_2b_opp","p_3b_p":"p_3b_opp","p_hr_p":"p_hr_opp"
    })

    # Merge batter priors + opponent
    bat = bat.merge(bat_rates, on="player_id", how="left")
    bat = bat.merge(opp, on=["game_id","team_id"], how="left")

    need = ["p_k_b","p_bb_b","p_1b_b","p_2b_b","p_3b_b","p_hr_b",
            "p_k_opp","p_bb_opp","p_1b_opp","p_2b_opp","p_3b_opp","p_hr_opp"]
    if bat[need].isna().any().any():
        bat.loc[bat[need].isna().any(axis=1),
                ["player_id","game_id","team_id","team","opp_pitcher_id"]+need].to_csv(
            SUM_DIR / "missing_rates_after_join_bat.csv", index=False
        )
        raise RuntimeError("Null rates after joins; see summaries/07_final/missing_rates_after_join_bat.csv")

    # League rates from season
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa),
        "bb": float(bat_s["walk"].sum()      / lg_pa),
        "1b": float(bat_s["single"].sum()    / lg_pa),
        "2b": float(bat_s["double"].sum()    / lg_pa),
        "3b": float(bat_s["triple"].sum()    / lg_pa),
        "hr": float(bat_s["home_run"].sum()  / lg_pa),
    }

    # Blend + apply environment
    bat["p_k"]  = log5(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
    bat["p_bb"] = log5(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
    bat["p_1b"] = log5(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"]) * bat["adj_woba_combined"]
    bat["p_2b"] = log5(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"]) * bat["adj_woba_combined"]
    bat["p_3b"] = log5(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"]) * bat["adj_woba_combined"]
    bat["p_hr"] = log5(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"]) * bat["adj_woba_combined"]

    # Clamp + outs
    for c in ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]:
        bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    over = s > 1.0
    if over.any():
        bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]] = \
            bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].div(s[over], axis=0)
    s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    bat["p_out"] = (1.0 - s).clip(0.0, 1.0)

    # Linear-weights -> expected runs
    LW = {"BB":0.33,"1B":0.47,"2B":0.77,"3B":1.04,"HR":1.40,"OUT":0.0}
    bat["runs_per_pa"] = (
        bat["p_bb"]*LW["BB"] + bat["p_1b"]*LW["1B"] + bat["p_2b"]*LW["2B"] +
        bat["p_3b"]*LW["3B"] + bat["p_hr"]*LW["HR"] + bat["p_out"]*LW["OUT"]
    )
    bat["expected_runs_batter"] = bat["runs_per_pa"] * bat["proj_pa_used"]

    keep_cols = [
        "player_id","name","team","team_id","game_id","proj_pa_used","opp_pitcher_id",
        "adj_woba_weather","adj_woba_park","adj_woba_combined",
        "p_k","p_bb","p_1b","p_2b","p_3b","p_hr","p_out",
        "runs_per_pa","expected_runs_batter"
    ]
    for c in keep_cols:
        if c not in bat.columns:
            bat[c] = pd.NA
    out = bat[keep_cols].copy().sort_values(["game_id","team_id","player_id"]).reset_index(drop=True)

    OUT_FILE_PROJ.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE_FINAL.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE_PROJ, index=False)
    out.to_csv(OUT_FILE_FINAL, index=False)

    write_text(SUM_DIR / "batter_event_probs_status.txt", f"OK batter_event_probs_daily rows={len(out)}")
    print(f"WROTE: {len(out)} rows -> {OUT_FILE_PROJ} AND {OUT_FILE_FINAL}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        write_text(SUM_DIR / "batter_event_probs_status.txt", "FAIL batter_event_probs_daily")
        write_text(SUM_DIR / "batter_event_probs_errors.txt", repr(e))
        raise
