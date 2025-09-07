#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pathlib import Path

# Paths
DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")
SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"
OUT_FILE        = OUT_DIR / "game_score_projections.csv"

ADJ_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

def require(df, cols, name):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing columns: {miss}")

def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def safe_rate(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce").replace(0, np.nan)
    return (n / d).fillna(0.0).clip(0.0)

def weighted_mean(g, cols, wcol):
    # weight-aware average; if total weight is 0, fall back to simple mean
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

def write_text(path: Path, text: str):
    path.write_text(text, encoding="utf-8")

def main():
    print("LOAD: daily and season inputs")
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
    to_num(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"])
    to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
    to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

    # ========= NEW: hard gate on game coverage (starters-only invariant) =========
    bat_games = set(pd.unique(bat_d["game_id"]))
    pit_games = set(pd.unique(pit_d["game_id"]))
    missing_games = sorted(list(bat_games - pit_games))
    if missing_games:
        # Emit explicit diagnostic and fail before we ever compute/join rates
        pd.DataFrame({"game_id": missing_games}).to_csv(
            SUM_DIR / "missing_opponent_starter_games.csv", index=False
        )
        raise RuntimeError(
            "One or more batter games lack an opponent starter in pitcher_props_projected_final.csv; "
            "see summaries/07_final/missing_opponent_starter_games.csv"
        )
    # ============================================================================
    print("KEY CHECK: (player_id, game_id) coverage for adjustments")
    keys_proj = set(zip(bat_d["player_id"], bat_d["game_id"]))
    keys_exp  = set(zip(bat_x["player_id"], bat_x["game_id"]))
    missing = keys_proj - keys_exp
    if missing:
        pd.DataFrame(list(missing), columns=["player_id","game_id"]).to_csv(
            SUM_DIR / "merge_mismatch_batters.csv", index=False
        )
        raise RuntimeError("Adjustment keys missing; see summaries/07_final/merge_mismatch_batters.csv")

    print("MERGE: drop preexisting adj columns from projected, then merge expanded")
    bat_d_noadj = bat_d.drop(columns=[c for c in ADJ_COLS if c in bat_d.columns], errors="ignore")
    bat = bat_d_noadj.merge(
        bat_x[["player_id","game_id"] + ADJ_COLS],
        on=["player_id","game_id"],
        how="inner"
    )

    for c in ADJ_COLS:
        if c not in bat.columns or bat[c].isna().any():
            bad = bat.loc[bat[c].isna()] if c in bat.columns else bat[["player_id","game_id"]]
            bad.to_csv(SUM_DIR / f"missing_{c}.csv", index=False)
            raise RuntimeError(f"{c} invalid after merge; see summaries/07_final/missing_{c}.csv")

    print("RATES: season priors and opponent starter (starters-only)")
    # Batter priors from season totals
    bat_rates = pd.DataFrame({
        "player_id": bat_s["player_id"],
        "p_k_b":  safe_rate(bat_s["strikeout"], bat_s["pa"]),
        "p_bb_b": safe_rate(bat_s["walk"],      bat_s["pa"]),
        "p_1b_b": safe_rate(bat_s["single"],    bat_s["pa"]),
        "p_2b_b": safe_rate(bat_s["double"],    bat_s["pa"]),
        "p_3b_b": safe_rate(bat_s["triple"],    bat_s["pa"]),
        "p_hr_b": safe_rate(bat_s["home_run"],  bat_s["pa"]),
    })

    # Opponent starter priors from season totals (map by pitcher_id)
    pit_rates = pd.DataFrame({
        "player_id": pit_s["player_id"],
        "p_k_p":  safe_rate(pit_s["strikeout"], pit_s["pa"]),
        "p_bb_p": safe_rate(pit_s["walk"],      pit_s["pa"]),
        "p_1b_p": safe_rate(pit_s["single"],    pit_s["pa"]),
        "p_2b_p": safe_rate(pit_s["double"],    pit_s["pa"]),
        "p_3b_p": safe_rate(pit_s["triple"],    pit_s["pa"]),
        "p_hr_p": safe_rate(pit_s["home_run"],  pit_s["pa"]),
    })

    # Attach pitcher season rates to each daily starter row
    pit_d_enh = pit_d.merge(pit_rates, on="player_id", how="left")

    # Build opponent map per (game_id, team_id_of_batters)
    # Each batter row belongs to team_id = T. Its opponent starter row is the one in pit_d where opponent_team_id == T for the same game_id.
    rate_cols = ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
    opp_rates = (
        pit_d_enh
        .groupby(["game_id","opponent_team_id"], as_index=False)
        .apply(lambda g: weighted_mean(g, rate_cols, "pa"), include_groups=False)
        .rename(columns={
            "opponent_team_id":"team_id",  # key for merging to batter's team
            "p_k_p":"p_k_opp","p_bb_p":"p_bb_opp","p_1b_p":"p_1b_opp",
            "p_2b_p":"p_2b_opp","p_3b_p":"p_3b_opp","p_hr_p":"p_hr_opp"
        })
    )

    print("JOIN: add batter season rates and opponent starter")
    bat = bat.merge(bat_rates, on="player_id", how="left")
    bat = bat.merge(opp_rates, on=["game_id","team_id"], how="left")

    need = ["p_k_b","p_bb_b","p_1b_b","p_2b_b","p_3b_b","p_hr_b",
            "p_k_opp","p_bb_opp","p_1b_opp","p_2b_opp","p_3b_opp","p_hr_opp"]
    if bat[need].isna().any().any():
        bat.loc[bat[need].isna().any(axis=1),
                ["player_id","game_id","team_id","team"]+need].to_csv(
            SUM_DIR / "missing_rates_after_join.csv", index=False
        )
        raise RuntimeError("Null outcome rates after join; see summaries/07_final/missing_rates_after_join.csv")

    print("LEAGUE: compute averages from season totals")
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa),
        "bb": float(bat_s["walk"].sum()      / lg_pa),
        "1b": float(bat_s["single"].sum()    / lg_pa),
        "2b": float(bat_s["double"].sum()    / lg_pa),
        "3b": float(bat_s["triple"].sum()    / lg_pa),
        "hr": float(bat_s["home_run"].sum()  / lg_pa),
    }

    print("LOG5 + ENV: blend and apply park/weather")
    bat["p_k"]  = log5(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
    bat["p_bb"] = log5(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
    bat["p_1b"] = log5(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"])
    bat["p_2b"] = log5(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"])
    bat["p_3b"] = log5(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"])
    bat["p_hr"] = log5(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"])

    for c in ["p_1b","p_2b","p_3b","p_hr"]:
        bat[c] = bat[c] * bat["adj_woba_combined"]

    print("CLAMP: probabilities and outs")
    for c in ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]:
        bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    over = s > 1.0
    if over.any():
        bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]] = \
            bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].div(s[over], axis=0)
        s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    bat["p_out"] = (1.0 - s).clip(0.0, 1.0)

    print("LW: linear weights to runs per PA and expected runs")
    LW = {"BB":0.33,"1B":0.47,"2B":0.77,"3B":1.04,"HR":1.40,"OUT":0.0}
    bat["runs_per_pa"] = (
        bat["p_bb"]*LW["BB"] + bat["p_1b"]*LW["1B"] + bat["p_2b"]*LW["2B"] +
        bat["p_3b"]*LW["3B"] + bat["p_hr"]*LW["HR"] + bat["p_out"]*LW["OUT"]
    )
    bat["expected_runs_batter"] = bat["runs_per_pa"] * bat["proj_pa_used"]

    print("AGG: to game/team")
    team = (
        bat.groupby(["game_id","team_id","team"], dropna=True)["expected_runs_batter"]
        .sum()
        .reset_index()
        .rename(columns={"expected_runs_batter":"expected_runs"})
        .sort_values(["game_id","team_id"])
        .reset_index(drop=True)
    )

    # Ensure an output file exists even if zero rows (no defaults used in calculations)
    team.to_csv(OUT_FILE, index=False)
    print(f"WROTE: {len(team)} rows -> {OUT_FILE}")

    # Write minimal status summary
    write_text(SUM_DIR / "status.txt", f"OK project_game_scores.py rows={len(team)}")
    write_text(SUM_DIR / "errors.txt", "")
    write_text(SUM_DIR / "summary.txt", f"rows={len(team)} out={OUT_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Always emit error files so CI captures artifacts even on failure
        SUM_DIR.mkdir(parents=True, exist_ok=True)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_text(SUM_DIR / "status.txt", "FAIL project_game_scores.py")
        write_text(SUM_DIR / "errors.txt", repr(e))
        write_text(SUM_DIR / "summary.txt", f"error={repr(e)}")
        raise
