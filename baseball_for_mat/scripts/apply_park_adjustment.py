#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_park_adjustment.py
"""
Adjust batter wOBA using park_factor. Source of truth:
- Prefer the 'park_factor' column if it already exists in the batter CSVs.
- Otherwise, fetch 'park_factor' from data/raw/todaysgames_normalized.csv.
No use of stadium_master.csv for park factors.
"""
import pandas as pd
from pathlib import Path

BH = Path("data/adjusted/batters_home.csv")
BA = Path("data/adjusted/batters_away.csv")
G  = Path("data/raw/todaysgames_normalized.csv")  # has: game_id, home_team_id, away_team_id, park_factor

OUT_H = Path("data/adjusted/batters_home_park.csv")
OUT_A = Path("data/adjusted/batters_away_park.csv")

def _load_games() -> pd.DataFrame:
    g = pd.read_csv(G, dtype=str)
    needed = ["game_id", "home_team_id", "away_team_id", "park_factor"]
    miss = [c for c in needed if c not in g.columns]
    if miss:
        raise KeyError(f"{G}: missing columns {miss}")
    # normalize types for safe merges
    for c in ["game_id", "home_team_id", "away_team_id", "park_factor"]:
        g[c] = g[c].astype("string").fillna("")
    return g

def _ensure_woba(df: pd.DataFrame) -> pd.Series:
    # prefer actual woba, else fallback to xwoba, numeric
    if "woba" in df.columns:
        return pd.to_numeric(df["woba"], errors="coerce")
    if "xwoba" in df.columns:
        return pd.to_numeric(df["xwoba"], errors="coerce")
    raise KeyError("batters input missing both 'woba' and 'xwoba'")

def _attach_park(df: pd.DataFrame, side: str, games: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()

    # If batter file already has park_factor, just use it.
    if "park_factor" in x.columns:
        pf_num = pd.to_numeric(x["park_factor"], errors="coerce")
    else:
        # otherwise merge from todaysgames_normalized
        if "team_id" not in x.columns:
            raise KeyError("batters input missing 'team_id'")

        # normalize key types
        x["team_id"] = x["team_id"].astype("string").fillna("")
        if side == "home":
            merged = x.merge(
                games[["home_team_id", "park_factor"]].rename(columns={"home_team_id": "team_id"}),
                on="team_id",
                how="left",
            )
        else:
            merged = x.merge(
                games[["away_team_id", "park_factor"]].rename(columns={"away_team_id": "team_id"}),
                on="team_id",
                how="left",
            )
        x = merged
        pf_num = pd.to_numeric(x["park_factor"], errors="coerce")

    # compute adjustment
    woba_num = _ensure_woba(x)
    x["adj_woba_park"] = woba_num * (pf_num / 100.0)

    return x

def main():
    bh = pd.read_csv(BH, dtype=str)
    ba = pd.read_csv(BA, dtype=str)
    games = _load_games()

    out_h = _attach_park(bh, "home", games)
    out_a = _attach_park(ba, "away", games)

    OUT_H.parent.mkdir(parents=True, exist_ok=True)
    out_h.to_csv(OUT_H, index=False)
    out_a.to_csv(OUT_A, index=False)

if __name__ == "__main__":
    main()
