#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pathlib import Path

DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
OUT_DIR = DAILY_DIR
END_DIR = Path("data/end_chain/final")
OUT_DIR.mkdir(parents=True, exist_ok=True)
END_DIR.mkdir(parents=True, exist_ok=True)

PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"

OUT_FILE_PROJ   = OUT_DIR / "pitcher_event_probabilities.csv"
OUT_FILE_FINAL  = END_DIR / "pitcher_event_probabilities.csv"

def safe_rate(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce").replace(0, np.nan)
    return (n / d).fillna(0.0).clip(0.0)

def main():
    pit = pd.read_csv(PITCHERS_DAILY)
    bat = pd.read_csv(BATTERS_DAILY)
    bat_s = pd.read_csv(BATTERS_SEASON)

    # Minimal requirements
    for df, name, cols in [
        (pit, str(PITCHERS_DAILY), ["player_id","game_id","team_id","opponent_team_id","pa"]),
        (bat, str(BATTERS_DAILY),  ["player_id","team_id","game_id","proj_pa_used"]),
        (bat_s, str(BATTERS_SEASON), ["player_id","pa","strikeout","walk","single","double","triple","home_run"]),
    ]:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise RuntimeError(f"{name} missing columns: {missing}")

    # Opposing lineup PA by team/game (weight for averaging)
    weights = (
        bat.groupby(["game_id","team_id"], as_index=False)["proj_pa_used"].sum()
        .rename(columns={"team_id":"opponent_team_id","proj_pa_used":"opp_pa_weight"})
    )

    # Batter league priors (as what pitcher faces)
    bat_rates = pd.DataFrame({
        "player_id": bat_s["player_id"],
        "p_k_b":  safe_rate(bat_s["strikeout"], bat_s["pa"]),
        "p_bb_b": safe_rate(bat_s["walk"],      bat_s["pa"]),
        "p_1b_b": safe_rate(bat_s["single"],    bat_s["pa"]),
        "p_2b_b": safe_rate(bat_s["double"],    bat_s["pa"]),
        "p_3b_b": safe_rate(bat_s["triple"],    bat_s["pa"]),
        "p_hr_b": safe_rate(bat_s["home_run"],  bat_s["pa"]),
    })
    # League averages
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa) if lg_pa > 0 else 0.0,
        "bb": float(bat_s["walk"].sum()      / lg_pa) if lg_pa > 0 else 0.0,
        "1b": float(bat_s["single"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "2b": float(bat_s["double"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "3b": float(bat_s["triple"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "hr": float(bat_s["home_run"].sum()  / lg_pa) if lg_pa > 0 else 0.0,
    }

    # Use league rates as default “lineup quality”; could be refined later
    pit_out = pit[["player_id","game_id","team_id","opponent_team_id","pa"]].copy()
    pit_out = pit_out.merge(weights, on=["game_id","opponent_team_id"], how="left")

    # Expected event rates per PA the pitcher allows (against average lineup)
    pit_out["p_k_allowed"]  = lg["k"]
    pit_out["p_bb_allowed"] = lg["bb"]
    pit_out["p_1b_allowed"] = lg["1b"]
    pit_out["p_2b_allowed"] = lg["2b"]
    pit_out["p_3b_allowed"] = lg["3b"]
    pit_out["p_hr_allowed"] = lg["hr"]
    s = pit_out[["p_k_allowed","p_bb_allowed","p_1b_allowed","p_2b_allowed","p_3b_allowed","p_hr_allowed"]].sum(axis=1)
    pit_out["p_out_allowed"] = (1.0 - s).clip(0.0, 1.0)

    pit_out.to_csv(OUT_FILE_PROJ, index=False)
    pit_out.to_csv(OUT_FILE_FINAL, index=False)
    print(f"OK: wrote {OUT_FILE_PROJ} and {OUT_FILE_FINAL} rows={len(pit_out)}")

if __name__ == "__main__":
    main()
