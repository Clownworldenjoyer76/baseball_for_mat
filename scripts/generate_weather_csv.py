#!/usr/bin/env python3
"""
Build data/weather_input.csv from normalized games + stadium metadata.

Inputs
- data/raw/todaysgames_normalized.csv
- data/Data/stadium_metadata.csv

Output
- data/weather_input.csv
"""
import pandas as pd
from pathlib import Path

GAMES = Path("data/raw/todaysgames_normalized.csv")
STAD  = Path("data/Data/stadium_metadata.csv")
OUT   = Path("data/weather_input.csv")

def main():
    g = pd.read_csv(GAMES)
    s = pd.read_csv(STAD)

    # Expect canonical names to match stadium_metadata.home_team
    g = g.rename(columns={"home_team_canonical":"home_team","away_team_canonical":"away_team"})
    cols = ["home_team","away_team","game_time","date"]
    g = g[cols]

    # Minimal stadium fields required downstream
    keep = ["home_team","venue","city","latitude","longitude","roof_type","time_of_day","Park Factor"]
    s = s[keep]

    x = g.merge(s, on="home_team", how="left")
    # Surface missing required coords
    req = ["venue","city","latitude","longitude","game_time"]
    missing = x[req].isna().any(axis=1)
    if missing.any():
        print("⚠️ Warning: missing values detected in:", ", ".join(req))
        print(x.loc[missing, ["home_team","away_team"] + req].head(1).to_string(index=False))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    x.to_csv(OUT, index=False)

if __name__ == "__main__":
    main()
