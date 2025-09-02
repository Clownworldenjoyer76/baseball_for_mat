#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_park_adjustment.py
"""
Adjust batter wOBA using Park Factor from data/raw/todaysgames_normalized.csv.

• Joins by team_id to the host park (home team).
• Never uses 'date'.
• Avoids 'park_factor' column collision by dropping any batter-side park_factor
  and reading the games-side value via an explicit suffix.
"""

import pandas as pd
from pathlib import Path

BH = Path("data/adjusted/batters_home.csv")
BA = Path("data/adjusted/batters_away.csv")
G  = Path("data/raw/todaysgames_normalized.csv")  # home_team_id, away_team_id, park_factor

OUT_H = Path("data/adjusted/batters_home_park.csv")
OUT_A = Path("data/adjusted/batters_away_park.csv")

def _prep_games() -> pd.DataFrame:
    g = pd.read_csv(G, dtype=str)
    req = ["home_team_id", "away_team_id", "park_factor"]
    missing = [c for c in req if c not in g.columns]
    if missing:
        raise KeyError(f"todaysgames_normalized missing {missing}")
    return g[["home_team_id", "away_team_id", "park_factor"]].copy()

def _ensure_woba(df: pd.DataFrame) -> pd.Series:
    if "woba" in df.columns:
        return pd.to_numeric(df["woba"], errors="coerce")
    # fallback to xwoba if needed
    return pd.to_numeric(df.get("xwoba", 0), errors="coerce")

def _attach(df: pd.DataFrame, side: str) -> pd.DataFrame:
    games = _prep_games()

    if "team_id" not in df.columns:
        raise KeyError("batters input missing required column: 'team_id'")

    # Remove any batter-side park_factor to avoid suffix collisions
    if "park_factor" in df.columns:
        df = df.drop(columns=["park_factor"])

    if side == "home":
        x = df.merge(
            games.rename(columns={"park_factor": "park_factor_games"})[["home_team_id", "park_factor_games"]],
            left_on="team_id",
            right_on="home_team_id",
            how="left",
        )
    else:
        x = df.merge(
            games.rename(columns={"park_factor": "park_factor_games"})[["away_team_id", "home_team_id", "park_factor_games"]],
            left_on="team_id",
            right_on="away_team_id",
            how="left",
        )
        # visiting team uses the host's (home) park; park_factor_games already matches host

    # Compute adjusted wOBA using the games-side park factor
    woba = _ensure_woba(x)
    pf = pd.to_numeric(x["park_factor_games"], errors="coerce")
    x["adj_woba_park"] = woba * (pf / 100.0)

    # Clean temporary merge keys
    drop_cols = [c for c in ["home_team_id", "away_team_id", "park_factor_games"] if c in x.columns]
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
