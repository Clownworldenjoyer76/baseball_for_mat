#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pathlib import Path

DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
RAW_DIR = Path("data/raw")
SUM_DIR = Path("summaries/07_final")
OUT_DIR = DAILY_DIR
END_DIR = Path("data/end_chain/final")
OUT_DIR.mkdir(parents=True, exist_ok=True)
END_DIR.mkdir(parents=True, exist_ok=True)
SUM_DIR.mkdir(parents=True, exist_ok=True)

PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"
TGN_CSV         = RAW_DIR / "todaysgames_normalized.csv"

OUT_FILE_PROJ   = OUT_DIR / "pitcher_event_probabilities.csv"
OUT_FILE_FINAL  = END_DIR / "pitcher_event_probabilities.csv"

def safe_rate(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce").replace(0, np.nan)
    return (n / d).fillna(0.0).clip(0.0)

def build_team_to_game_map(tgn: pd.DataFrame) -> pd.DataFrame:
    need = {"game_id", "home_team_id", "away_team_id"}
    missing = sorted(list(need - set(tgn.columns)))
    if missing:
        raise RuntimeError(f"{TGN_CSV} missing columns: {missing}")
    tgn = tgn[["game_id", "home_team_id", "away_team_id"]].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()
    home = tgn.rename(columns={"home_team_id": "team_id"})[["game_id", "team_id"]]
    away = tgn.rename(columns={"away_team_id": "team_id"})[["game_id", "team_id"]]
    team_game = pd.concat([home, away], ignore_index=True).drop_duplicates()
    # Validate two teams per game
    per_game = team_game.groupby("game_id")["team_id"].nunique()
    bad = per_game[per_game != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} invalid two-team constraint: {bad.to_dict()}")
    return team_game

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

    # Canonicalize game_id from the same slate map used by batters
    tgn = pd.read_csv(TGN_CSV, dtype=str)
    team_game = build_team_to_game_map(tgn)

    # Override/patch game_id based on team_id -> game_id mapping (coalesce)
    pit = pit.merge(team_game, on="team_id", how="left", suffixes=("", "_from_map"))
    pit["game_id"] = pit["game_id"].where(pit["game_id"].notna(), pit["game_id_from_map"])
    pit.drop(columns=[c for c in ["game_id_from_map"] if c in pit.columns], inplace=True)

    # Opposing lineup PA weight by game/opponent
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
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa) if lg_pa > 0 else 0.0,
        "bb": float(bat_s["walk"].sum()      / lg_pa) if lg_pa > 0 else 0.0,
        "1b": float(bat_s["single"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "2b": float(bat_s["double"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "3b": float(bat_s["triple"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "hr": float(bat_s["home_run"].sum()  / lg_pa) if lg_pa > 0 else 0.0,
    }

    pit_out = pit[["player_id","game_id","team_id","opponent_team_id","pa"]].copy()
    pit_out = pit_out.merge(weights, on=["game_id","opponent_team_id"], how="left")

    # Expected event rates per PA allowed (league-average lineup baseline)
    pit_out["p_k_allowed"]  = lg["k"]
    pit_out["p_bb_allowed"] = lg["bb"]
    pit_out["p_1b_allowed"] = lg["1b"]
    pit_out["p_2b_allowed"] = lg["2b"]
    pit_out["p_3b_allowed"] = lg["3b"]
    pit_out["p_hr_allowed"] = lg["hr"]
    s = pit_out[["p_k_allowed","p_bb_allowed","p_1b_allowed","p_2b_allowed","p_3b_allowed","p_hr_allowed"]].sum(axis=1)
    pit_out["p_out_allowed"] = (1.0 - s).clip(0.0, 1.0)

    # Final guards — ensure we’re aligned on the same slate as batters
    bat_games = set(pd.to_numeric(bat["game_id"], errors="coerce").dropna().astype(int).tolist())
    pit_games = set(pd.to_numeric(pit_out["game_id"], errors="coerce").dropna().astype(int).tolist())
    if bat_games != pit_games:
        diff_a = sorted(list(bat_games - pit_games))
        diff_b = sorted(list(pit_games - bat_games))
        (SUM_DIR / "pitcher_batter_slate_diff.txt").write_text(
            f"bat_not_in_pit={diff_a}\npit_not_in_bat={diff_b}\n", encoding="utf-8"
        )
        raise RuntimeError(f"Slate mismatch (batters vs pitchers). See {SUM_DIR/'pitcher_batter_slate_diff.txt'}")

    pit_out.to_csv(OUT_FILE_PROJ, index=False)
    pit_out.to_csv(OUT_FILE_FINAL, index=False)
    print(f"OK: wrote {OUT_FILE_PROJ} and {OUT_FILE_FINAL} rows={len(pit_out)}")

if __name__ == "__main__":
    main()
