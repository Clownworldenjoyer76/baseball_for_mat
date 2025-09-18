#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pathlib import Path

DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
RAW_DIR    = Path("data/raw")
SUM_DIR    = Path("summaries/07_final")
OUT_DIR    = Path("data/_projections")
END_DIR    = Path("data/end_chain/final")
SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
END_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"
TGN_CSV         = RAW_DIR / "todaysgames_normalized.csv"

OUT_FILE_PROJ   = OUT_DIR / "batter_event_probabilities.csv"
OUT_FILE_FINAL  = END_DIR / "batter_event_probabilities.csv"

ADJ_COLS = ["adj_woba_weather","adj_woba_park","adj_woba_combined"]

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

def log5(b, p, lg):
    if lg <= 0:
        return pd.Series(0.0, index=b.index)
    b = pd.to_numeric(b, errors="coerce").fillna(0.0)
    p = pd.to_numeric(p, errors="coerce").fillna(0.0)
    return (b * p) / lg

def build_team_to_game_map(tgn: pd.DataFrame) -> pd.DataFrame:
    need = {"game_id","home_team_id","away_team_id"}
    miss = sorted(list(need - set(tgn.columns)))
    if miss:
        raise RuntimeError(f"{TGN_CSV} missing columns: {miss}")
    tgn = tgn[["game_id","home_team_id","away_team_id"]].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()
    home = tgn.rename(columns={"home_team_id":"team_id"})[["game_id","team_id"]]
    away = tgn.rename(columns={"away_team_id":"team_id"})[["game_id","team_id"]]
    team_game = pd.concat([home, away], ignore_index=True).drop_duplicates()
    pg = team_game.groupby("game_id")["team_id"].nunique()
    bad = pg[pg != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} invalid two-team constraint: {bad.to_dict()}")
    return team_game

def main():
    print("LOAD: daily & season inputs")
    bat_d = pd.read_csv(BATTERS_DAILY)
    bat_x = pd.read_csv(BATTERS_EXP)
    pit_d = pd.read_csv(PITCHERS_DAILY)
    bat_s = pd.read_csv(BATTERS_SEASON)
    pit_s = pd.read_csv(PITCHERS_SEASON)

    require(bat_d, ["player_id","team_id","team","game_id","proj_pa_used"], str(BATTERS_DAILY))
    require(bat_x, ["player_id","game_id"] + [c for c in ADJ_COLS if c in bat_x.columns], str(BATTERS_EXP))
    require(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"], str(PITCHERS_DAILY))
    require(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(BATTERS_SEASON))
    require(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(PITCHERS_SEASON))

    # Assert non-null keys (prep should guarantee it)
    if bat_d["team_id"].isna().any() or bat_d["team_id"].astype(str).eq("").any():
        bad = bat_d.loc[bat_d["team_id"].isna() | (bat_d["team_id"].astype(str)==""),
                        ["player_id","team","team_id","game_id"]]
        bad.to_csv(SUM_DIR / "batter_daily_missing_team_id.csv", index=False)
        raise AssertionError("batter_props_projected_final.csv has missing team_id")
    if bat_d["game_id"].isna().any() or bat_d["game_id"].astype(str).eq("").any():
        bad = bat_d.loc[bat_d["game_id"].isna() | (bat_d["game_id"].astype(str)==""),
                        ["player_id","team","team_id","game_id"]]
        bad.to_csv(SUM_DIR / "batter_daily_missing_game_id.csv", index=False)
        raise AssertionError("batter_props_projected_final.csv has missing game_id")

    to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
    to_num(bat_x, ["player_id","game_id"])
    to_num(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"])
    to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
    to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

    # Normalize adjustments
    for c in ADJ_COLS:
        if c not in bat_x.columns:
            bat_x[c] = 1.0
    bat_x[ADJ_COLS] = bat_x[ADJ_COLS].apply(pd.to_numeric, errors="coerce").fillna(1.0)

    # (Rest unchanged from your previous version)
    keys_proj = set(zip(bat_d["player_id"], bat_d["game_id"]))
    keys_exp  = set(zip(bat_x["player_id"], bat_x["game_id"]))
    missing = keys_proj - keys_exp
    if missing:
        pd.DataFrame(list(missing), columns=["player_id","game_id"]).to_csv(
            SUM_DIR / "merge_mismatch_batters.csv", index=False
        )
        print("[WARN] some (player_id, game_id) not present in expanded; defaulting adj_woba_* = 1.0 for those rows.")
        miss_df = pd.DataFrame(list(missing), columns=["player_id","game_id"])
        for c in ADJ_COLS:
            miss_df[c] = 1.0
        bat_x = pd.concat([bat_x, miss_df], ignore_index=True)

    bat = bat_d.drop(columns=[c for c in ADJ_COLS if c in bat_d.columns], errors="ignore") \
               .merge(bat_x[["player_id","game_id"] + ADJ_COLS], on=["player_id","game_id"], how="left")
    for c in ADJ_COLS:
        bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(1.0).clip(lower=0)

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

    pit_d_enh = pit_d.merge(pit_rates, on="player_id", how="left")

    rate_cols = ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
    opp_rates = (
        pit_d_enh.groupby(["game_id","opponent_team_id"], as_index=False)
                 .apply(lambda g: pd.Series(
                     {c: float(pd.to_numeric(g[c], errors="coerce").fillna(0)
                               .mul(pd.to_numeric(g["pa"], errors="coerce").fillna(0)).sum()
                               / max(float(pd.to_numeric(g["pa"], errors="coerce").fillna(0).sum()), 1.0))
                      for c in rate_cols}),
                        include_groups=False)
                 .rename(columns={"opponent_team_id":"team_id"})
    )

    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa) if lg_pa > 0 else 0.0,
        "bb": float(bat_s["walk"].sum()      / lg_pa) if lg_pa > 0 else 0.0,
        "1b": float(bat_s["single"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "2b": float(bat_s["double"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "3b": float(bat_s["triple"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "hr": float(bat_s["home_run"].sum()  / lg_pa) if lg_pa > 0 else 0.0,
    }
    lg_row = pd.DataFrame([{
        "p_k_p":lg["k"], "p_bb_p":lg["bb"], "p_1b_p":lg["1b"],
        "p_2b_p":lg["2b"], "p_3b_p":lg["3b"], "p_hr_p":lg["hr"]
    }])

    bat = bat.merge(bat_rates, on="player_id", how="left")
    bat = bat.merge(opp_rates, on=["game_id","team_id"], how="left", suffixes=("","_opp"))

    for src, dst in zip(["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"],
                        ["p_k_opp","p_bb_opp","p_1b_opp","p_2b_opp","p_3b_opp","p_hr_opp"]):
        if dst not in bat.columns:
            bat[dst] = np.nan
        bat[dst] = bat[dst].fillna(lg_row.iloc[0][src])

    bat["p_k"]  = log5(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
    bat["p_bb"] = log5(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
    bat["p_1b"] = log5(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"])
    bat["p_2b"] = log5(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"])
    bat["p_3b"] = log5(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"])
    bat["p_hr"] = log5(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"])

    bat["p_1b"] *= bat["adj_woba_combined"]
    bat["p_2b"] *= bat["adj_woba_combined"]
    bat["p_3b"] *= bat["adj_woba_combined"]
    bat["p_hr"] *= bat["adj_woba_combined"]

    for c in ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]:
        bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    over = s > 1.0
    if over.any():
        bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]] = \
            bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].div(s[over], axis=0)
    bat["p_out"] = (1.0 - bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)).clip(0.0, 1.0)

    keep_cols = ["player_id","team_id","team","game_id","proj_pa_used",
                 "p_k","p_bb","p_1b","p_2b","p_3b","p_hr","p_out",
                 "adj_woba_weather","adj_woba_park","adj_woba_combined"]
    result = bat[keep_cols].copy()

    # Final guards
    if result["team_id"].isna().any() or result["team_id"].astype(str).eq("").any():
        result.loc[result["team_id"].isna() | (result["team_id"].astype(str)==""), ["player_id","team","game_id"]].to_csv(
            SUM_DIR / "bep_null_team_id.csv", index=False
        )
        raise AssertionError("batter_event_probabilities has missing team_id")
    if result["game_id"].isna().any() or result["game_id"].astype(str).eq("").any():
        result.loc[result["game_id"].isna() | (result["game_id"].astype(str)==""), ["player_id","team","team_id"]].to_csv(
            SUM_DIR / "bep_null_game_id.csv", index=False
        )
        raise AssertionError("batter_event_probabilities has missing game_id")

    result.to_csv(OUT_FILE_PROJ, index=False)
    result.to_csv(OUT_FILE_FINAL, index=False)
    print(f"OK: wrote {OUT_FILE_PROJ} and {OUT_FILE_FINAL} rows={len(result)}")

if __name__ == "__main__":
    main()
