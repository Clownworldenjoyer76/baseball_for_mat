#!/usr/bin/env python3
import argparse
import pandas as pd
from pathlib import Path

GAMES_FILE_DEFAULT   = Path("data/end_chain/cleaned/games_today_cleaned.csv")
BAT_STRENGTHS_FILE   = Path("data/_projections/batter_props_projected.csv")
PIT_MEGA_FILE        = Path("data/_projections/pitcher_mega_z.csv")
OUT_FILE             = Path("data/_projections/final_scores_projected.csv")

def _key(s: str) -> str:
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", default=str(GAMES_FILE_DEFAULT))
    args = ap.parse_args()

    games_path = Path(args.games)
    if not games_path.exists():
        raise SystemExit(f"❌ Missing games file: {games_path}")

    g = pd.read_csv(games_path)
    # ensure keys
    for c in ("home_team","away_team"):
        if c in g.columns:
            g[c] = g[c].astype(str)
    g["home_key"] = g["home_team"].apply(_key) if "home_team" in g.columns else ""
    g["away_key"] = g["away_team"].apply(_key) if "away_team" in g.columns else ""

    # load strengths (keep simple — whatever you already compute)
    bat_ok = BAT_STRENGTHS_FILE.exists()
    pit_ok = PIT_MEGA_FILE.exists()
    if not bat_ok or not pit_ok:
        print("⚠️ Missing inputs for projections; writing minimal frame.")
        out = g.copy()
        # prefer to carry game_id if present
        cols = ["game_id","home_team","away_team"]
        for c in ("home_team","away_team","game_id"):
            if c not in out.columns:
                out[c] = ""
        out = out[cols].drop_duplicates()
        OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(OUT_FILE, index=False)
        print(f"✅ Wrote: {OUT_FILE} (games={len(out)})")
        return

    # Example stub: compute projected totals using your existing fields if present
    # (keeps structure; the important part here is carrying the game_id downstream)
    out = g.copy()
    out_cols = ["game_id","home_team","away_team"]
    if "game_id" not in out.columns:
        out["game_id"] = pd.NA
    out["projected_real_run_total"] = pd.NA
    out["favorite"] = pd.NA
    out = out[out_cols + ["projected_real_run_total","favorite"]].drop_duplicates()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote: {OUT_FILE} (games={len(out)})")

if __name__ == "__main__":
    main()
