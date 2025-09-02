#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_park_adjustment.py
"""
Adjust batter wOBA using Park Factor from todaysgames_normalized.csv.
Join by team_id. No date usage. No stadium_master dependency for PF.
"""
import pandas as pd
from pathlib import Path

BH = Path("data/adjusted/batters_home.csv")
BA = Path("data/adjusted/batters_away.csv")
G  = Path("data/raw/todaysgames_normalized.csv")  # home_team_id, away_team_id, park_factor

OUT_H = Path("data/adjusted/batters_home_park.csv")
OUT_A = Path("data/adjusted/batters_away_park.csv")

def _prep_games():
    g = pd.read_csv(G, dtype=str)
    req = ["home_team_id", "away_team_id", "park_factor"]
    miss = [c for c in req if c not in g.columns]
    if miss:
        raise KeyError(f"todaysgames_normalized missing {miss}")
    return g[["home_team_id", "away_team_id", "park_factor"]]

def _attach(df: pd.DataFrame, side: str) -> pd.DataFrame:
    games = _prep_games()

    if "team_id" not in df.columns:
        raise KeyError("batters input missing team_id")

    if side == "home":
        x = df.merge(
            games[["home_team_id", "park_factor"]],
            left_on="team_id",
            right_on="home_team_id",
            how="left"
        )
    else:
        x = df.merge(
            games[["away_team_id", "home_team_id", "park_factor"]],
            left_on="team_id",
            right_on="away_team_id",
            how="left"
        )
        # park_factor already corresponds to the host (home_team_id)

    if "woba" not in x.columns:
        x["woba"] = pd.to_numeric(x.get("xwoba", 0), errors="coerce")

    pf = pd.to_numeric(x["park_factor"], errors="coerce")
    woba = pd.to_numeric(x["woba"], errors="coerce")
    x["adj_woba_park"] = woba * (pf / 100.0)

    drop_cols = [c for c in ["home_team_id", "away_team_id"] if c in x.columns and c not in df.columns]
    x = x.drop(columns=drop_cols)
    return x

def main():
    bh = pd.read_csv(BH, dtype=str)
    ba = pd.read_csv(BA, dtype=str)

    out_h = _attach(bh, "home")
    out_a = _attach(ba, "away")

    OUT_H.parent.mkdir(parents=True, exist_ok=True)
    out_h.to_csv(OUT_H, index=False)
    out_a.to_csv(OUT_A, index=False)

if __name__ == "__main__":
    main()
