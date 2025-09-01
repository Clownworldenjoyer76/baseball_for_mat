#!/usr/bin/env python3
"""
Post-normalization fixups:
- Clean batters_today.csv (remove pitchers, enforce team_id int, dedupe by player_id).
- Clean pitchers_normalized_cleaned.csv (keep pitchers only, enforce team_id int).
- Do NOT add game_id (that now comes from todaysgames.py).
"""

import argparse
import pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", required=True, help="todaysgames_normalized.csv")
    ap.add_argument("--batters", required=True, help="batters_today.csv")
    ap.add_argument("--pitchers", required=True, help="pitchers_normalized_cleaned.csv")
    ap.add_argument("--game-date", required=True, help="YYYYMMDD (unused, kept for compatibility)")
    args = ap.parse_args()

    # --- games: load only (no game_id modification here)
    g = pd.read_csv(args.games, low_memory=False)
    for col in ["home_team_id","away_team_id"]:
        if col in g.columns:
            g[col] = pd.to_numeric(g[col], errors="coerce").astype("Int64")
    g.to_csv(args.games, index=False)

    # Build set of valid team_ids for today
    valid_team_ids = set(pd.concat([
        g.get("home_team_id", pd.Series([], dtype="Int64")),
        g.get("away_team_id", pd.Series([], dtype="Int64"))
    ]).dropna().astype(int).tolist())

    # --- batters
    b = pd.read_csv(args.batters, low_memory=False)
    if "type" in b.columns:
        b = b[b["type"].str.lower().eq("batter")]
    if "team_id" in b.columns:
        b["team_id"] = pd.to_numeric(b["team_id"], errors="coerce").astype("Int64")
        b = b[b["team_id"].isin(valid_team_ids)]
    if "player_id" in b.columns:
        if "pa" in b.columns:
            b = b.sort_values(by=["pa"], ascending=False).drop_duplicates("player_id", keep="first")
        else:
            b = b.drop_duplicates("player_id", keep="first")
    b.to_csv(args.batters, index=False)

    # --- pitchers
    p = pd.read_csv(args.pitchers, low_memory=False)
    if "type" in p.columns:
        p = p[p["type"].str.lower().eq("pitcher")]
    if "team_id" in p.columns:
        p["team_id"] = pd.to_numeric(p["team_id"], errors="coerce").astype("Int64")
    p.to_csv(args.pitchers, index=False)

    print("✅ post_normalize_fixups complete (no game_id injected)")

if __name__ == "__main__":
    main()
