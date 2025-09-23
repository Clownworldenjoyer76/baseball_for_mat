#!/usr/bin/env python3
# Inject game/team context into pitcher projections from data/raw/todaysgames_normalized.csv

from __future__ import annotations
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TGN = ROOT / "data" / "raw" / "todaysgames_normalized.csv"

PROJ_CANDIDATES = [
    ROOT / "data" / "_projections" / "pitcher_props_projected_final.csv",
    ROOT / "data" / "_projections" / "pitcher_props_projected.csv",
]

OUT = ROOT / "data" / "_projections" / "pitcher_props_projected_final.csv"

REQ_TGN = [
    "game_id","home_team_id","away_team_id",
    "pitcher_home_id","pitcher_away_id"
]
CTX_COLS = ["game_id","team_id","opponent_team_id"]

def clean_id(x: str) -> str:
    if x is None: return "UNKNOWN"
    s = str(x).strip()
    if s == "" or s.lower() == "nan": return "UNKNOWN"
    # strip trailing .0 if it looks like a floaty ID
    if s.endswith(".0"):
        try:
            return str(int(float(s)))
        except Exception:
            pass
    return s

def load_proj():
    for p in PROJ_CANDIDATES:
        if p.exists():
            return pd.read_csv(p, dtype=str).fillna("UNKNOWN"), p
    raise FileNotFoundError("No pitcher projections found in expected locations.")

def main():
    # inputs
    tgn = pd.read_csv(TGN, dtype=str)
    missing = [c for c in REQ_TGN if c not in tgn.columns]
    if missing:
        raise RuntimeError(f"{TGN} missing required columns: {missing}")

    # long-form context from TGN
    home = tgn.rename(columns={
        "pitcher_home_id":"player_id",
        "home_team_id":"team_id",
        "away_team_id":"opponent_team_id",
    })[["game_id","team_id","opponent_team_id","player_id"]].copy()

    away = tgn.rename(columns={
        "pitcher_away_id":"player_id",
        "away_team_id":"team_id",
        "home_team_id":"opponent_team_id",
    })[["game_id","team_id","opponent_team_id","player_id"]].copy()

    ctx = pd.concat([home, away], ignore_index=True)

    # clean IDs and force strings
    for c in ["player_id","team_id","opponent_team_id","game_id"]:
        ctx[c] = ctx[c].map(clean_id).astype(str)

    # drop UNKNOWN player rows; keep one row per pitcher
    ctx = ctx[ctx["player_id"] != "UNKNOWN"].drop_duplicates(subset=["player_id"])

    # load projections
    proj, used_path = load_proj()
    if "player_id" not in proj.columns:
        raise RuntimeError(f"{used_path} missing 'player_id' column.")

    proj["player_id"] = proj["player_id"].map(clean_id)

    # merge context
    merged = proj.merge(ctx, on="player_id", how="left", suffixes=("", "_tgn"))

    # ensure CTX_COLS exist and fill
    for c in CTX_COLS:
        if c not in merged.columns:
            merged[c] = merged.get(f"{c}_tgn", "UNKNOWN")
        merged[c] = merged[c].fillna(merged.get(f"{c}_tgn")).fillna("UNKNOWN").astype(str)

    # final cleanup
    for c in merged.columns:
        merged[c] = merged[c].fillna("UNKNOWN").astype(str)

    # stats
    filled = (merged[CTX_COLS] != "UNKNOWN").all(axis=1).sum()
    total = len(merged)
    print(f"Injected context for {filled}/{total} pitchers using {TGN}")

    # write
    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT, index=False)
    print(f"Wrote: {OUT} (rows={len(merged)})")

if __name__ == "__main__":
    main()
