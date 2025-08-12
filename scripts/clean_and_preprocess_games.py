#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

RAW_GAMES   = Path("data/end_chain/first/games_today.csv")         # your existing source
CLEAN_OUT   = Path("data/end_chain/cleaned/games_today_cleaned.csv")
MLB_IDS_CSV = Path("data/raw/mlb_game_ids.csv")                    # new join source

def _key(s: str) -> str:
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

def main():
    if not RAW_GAMES.exists():
        raise SystemExit(f"❌ missing input: {RAW_GAMES}")

    g = pd.read_csv(RAW_GAMES)
    # normalize column names you already use
    g.columns = g.columns.str.strip()
    # ensure we have home_team/away_team
    rename_map = {"Home":"home_team","home":"home_team","Away":"away_team","away":"away_team"}
    for k,v in rename_map.items():
        if k in g.columns and v not in g.columns:
            g = g.rename(columns={k:v})

    needed = {"home_team","away_team"}
    if not needed.issubset(g.columns):
        raise SystemExit(f"❌ games_today missing {sorted(needed - set(g.columns))}")

    g = g.copy()
    g["home_key"] = g["home_team"].apply(_key)
    g["away_key"] = g["away_team"].apply(_key)

    # attach MLB game_pk if available
    if MLB_IDS_CSV.exists():
        ids = pd.read_csv(MLB_IDS_CSV)
        ids = ids.rename(columns={"date":"mlb_date"})
        ids["home_key"] = ids["home_team"].apply(_key)
        ids["away_key"] = ids["away_team"].apply(_key)
        g = g.merge(
            ids[["mlb_date","home_key","away_key","game_pk","game_number","game_datetime"]],
            on=["home_key","away_key"],
            how="left"
        )
        g["game_id"] = g["game_pk"].astype("Int64").astype("string")
    else:
        g["game_id"] = pd.NA

    # keep your existing columns plus new identifiers
    keep_order = ["game_id","home_team","away_team","game_datetime","game_number"]
    others = [c for c in g.columns if c not in keep_order]
    out = g[keep_order + others]

    CLEAN_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(CLEAN_OUT, index=False)
    print(f"✅ Saved cleaned game data to: {CLEAN_OUT}")

if __name__ == "__main__":
    main()
