#!/usr/bin/env python3
"""
Apply park-factor adjustment to batter wOBA using manual park factors.

Inputs
- data/adjusted/batters_home.csv
- data/adjusted/batters_away.csv
- data/raw/todaysgames_normalized.csv  (home_team_id/home_team_canonical)
- data/Data/stadium_metadata.csv       (Park Factor chosen)

Outputs
- data/adjusted/batters_home_park.csv
- data/adjusted/batters_away_park.csv
"""
import pandas as pd
from pathlib import Path

BH = Path("data/adjusted/batters_home.csv")
BA = Path("data/adjusted/batters_away.csv")
G  = Path("data/raw/todaysgames_normalized.csv")
ST = Path("data/Data/stadium_metadata.csv")

OUT_H = Path("data/adjusted/batters_home_park.csv")
OUT_A = Path("data/adjusted/batters_away_park.csv")

def _prep_games():
    g = pd.read_csv(G)
    g = g.rename(columns={
        "home_team_canonical":"home_team",
        "away_team_canonical":"away_team"
    })
    return g[["home_team","away_team","home_team_id","away_team_id","date"]]

def _attach(df: pd.DataFrame, side: str) -> pd.DataFrame:
    games = _prep_games()
    stad  = pd.read_csv(ST)[["home_team","Park Factor"]]

    if side == "home":
        x = df.merge(games[["home_team","date"]], left_on="team", right_on="home_team", how="left")
    else:
        x = df.merge(games[["away_team","home_team","date"]],
                     left_on="team", right_on="away_team", how="left")
        # Repoint to park of the home team they visit
        x = x.rename(columns={"home_team":"host_home_team"})

    # Bring Park Factor by the host home team
    host_col = "home_team" if side == "home" else "host_home_team"
    x = x.merge(stad, left_on=host_col, right_on="home_team", how="left", suffixes=("",""))

    # Compute adjusted wOBA column (copy if missing)
    if "woba" not in x.columns:
        x["woba"] = pd.to_numeric(x.get("xwoba", 0), errors="coerce")

    pf = pd.to_numeric(x["Park Factor"], errors="coerce")
    x["adj_woba_park"] = pd.to_numeric(x["woba"], errors="coerce") * (pf / 100.0)

    # Housekeeping
    drop_cols = [c for c in ["home_team","away_team","host_home_team"] if c in x.columns]
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
