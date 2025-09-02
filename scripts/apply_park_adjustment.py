#!/usr/bin/env python3
"""
Apply park-factor adjustment to batter wOBA using manual park factors.

Inputs
- data/adjusted/batters_home.csv
- data/adjusted/batters_away.csv
- data/raw/todaysgames_normalized.csv   (contains home_team_id, away_team_id)
- data/manual/stadium_master.csv        (team_id, Park Factor)

Outputs
- data/adjusted/batters_home_park.csv
- data/adjusted/batters_away_park.csv
"""
import pandas as pd
from pathlib import Path

BH = Path("data/adjusted/batters_home.csv")
BA = Path("data/adjusted/batters_away.csv")
G  = Path("data/raw/todaysgames_normalized.csv")
ST = Path("data/manual/stadium_master.csv")

OUT_H = Path("data/adjusted/batters_home_park.csv")
OUT_A = Path("data/adjusted/batters_away_park.csv")

def _prep_games():
    g = pd.read_csv(G)
    return g[["home_team_id","away_team_id","date"]]

def _attach(df: pd.DataFrame, side: str) -> pd.DataFrame:
    games = _prep_games()
    stad  = pd.read_csv(ST)[["team_id","Park Factor"]]

    if side == "home":
        # join batter team_id to games.home_team_id
        x = df.merge(games[["home_team_id","date"]],
                     left_on="team_id", right_on="home_team_id", how="left")
        host_id_col = "home_team_id"
    else:
        # join batter team_id to games.away_team_id, map to host home_team_id
        x = df.merge(games[["away_team_id","home_team_id","date"]],
                     left_on="team_id", right_on="away_team_id", how="left")
        host_id_col = "home_team_id"

    # bring Park Factor by host team_id
    x = x.merge(stad, left_on=host_id_col, right_on="team_id", how="left", suffixes=("",""))

    # ensure woba exists
    if "woba" not in x.columns:
        x["woba"] = pd.to_numeric(x.get("xwoba", 0), errors="coerce")

    pf = pd.to_numeric(x["Park Factor"], errors="coerce")
    x["adj_woba_park"] = pd.to_numeric(x["woba"], errors="coerce") * (pf / 100.0)

    # housekeeping
    drop_cols = [c for c in ["home_team_id","away_team_id","team_id"] if c in x.columns and c not in df.columns]
    x = x.drop(columns=drop_cols)
    return x

def main():
    bh = pd.read_csv(BH)
    ba = pd.read_csv(BA)

    out_h = _attach(bh, "home")
    out_a = _attach(ba, "away")

    OUT_H.parent.mkdir(parents=True, exist_ok=True)
    out_h.to_csv(OUT_H, index=False)
    out_a.to_csv(OUT_A, index=False)

if __name__ == "__main__":
    main()
