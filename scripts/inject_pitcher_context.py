#!/usr/bin/env python3
# Injects game/team context into pitcher projections, robust to '518876.0' vs '518876'
from __future__ import annotations
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PROJ_IN   = ROOT / "data" / "_projections" / "pitcher_props_projected.csv"
SP_LONG   = ROOT / "data" / "raw" / "startingpitchers_with_opp_context.csv"
TGN_WIDE  = ROOT / "data" / "raw" / "todaysgames_normalized.csv"
PROJ_OUT  = ROOT / "data" / "_projections" / "pitcher_props_projected_final.csv"

REQ_PROJ = ["player_id"]
REQ_SP   = ["game_id","team_id","opponent_team_id","player_id"]
REQ_TGN  = ["game_id","home_team_id","away_team_id","pitcher_home_id","pitcher_away_id"]

def norm_id(x):
    if pd.isna(x): return None
    s = str(x).strip()
    # remove trailing .0 safely
    if s.endswith(".0"):
        s = s[:-2]
    return s if s and s.upper() != "NAN" else None

def norm_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(norm_id)
    return df

def must_have(df, cols, name):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing columns: {missing}")

def build_tgn_long(tgn: pd.DataFrame) -> pd.DataFrame:
    home = tgn[["game_id","home_team_id","away_team_id","pitcher_home_id"]].copy()
    home.rename(columns={
        "home_team_id":"team_id",
        "away_team_id":"opponent_team_id",
        "pitcher_home_id":"player_id"
    }, inplace=True)
    away = tgn[["game_id","home_team_id","away_team_id","pitcher_away_id"]].copy()
    away.rename(columns={
        "away_team_id":"team_id",
        "home_team_id":"opponent_team_id",
        "pitcher_away_id":"player_id"
    }, inplace=True)
    long = pd.concat([home, away], ignore_index=True)
    # normalize IDs again after renames
    long = norm_cols(long, ["game_id","team_id","opponent_team_id","player_id"])
    # drop rows with no player_id
    long = long.dropna(subset=["player_id"])
    return long[["game_id","team_id","opponent_team_id","player_id"]].drop_duplicates()

def main():
    # Load inputs
    proj = pd.read_csv(PROJ_IN, dtype=str)
    must_have(proj, REQ_PROJ, PROJ_IN)
    proj = norm_cols(proj, ["player_id"])

    # startingpitchers_with_opp_context (long)
    if SP_LONG.exists():
        sp = pd.read_csv(SP_LONG, dtype=str)
        must_have(sp, REQ_SP, SP_LONG)
        sp = norm_cols(sp, ["game_id","team_id","opponent_team_id","player_id"])
        sp = sp.dropna(subset=["player_id"])
        sp = sp[REQ_SP].drop_duplicates()
    else:
        sp = pd.DataFrame(columns=REQ_SP)

    # todaysgames_normalized (wide -> long)
    if TGN_WIDE.exists():
        tgn = pd.read_csv(TGN_WIDE, dtype=str)
        must_have(tgn, REQ_TGN, TGN_WIDE)
        tgn = norm_cols(tgn, REQ_TGN)
        tgn_long = build_tgn_long(tgn)
    else:
        tgn_long = pd.DataFrame(columns=REQ_SP)

    # Merge in two passes: prefer SP_LONG context, then fill from TGN
    merged = proj.merge(sp, on="player_id", how="left", suffixes=("", "_sp"))
    # identify which still need context
    need = merged["game_id"].isna() | merged["team_id"].isna() | merged["opponent_team_id"].isna()
    if need.any():
        merged = merged.merge(tgn_long.add_suffix("_tgn"), left_on="player_id", right_on="player_id_tgn", how="left")
        for col in ["game_id","team_id","opponent_team_id"]:
            merged[col] = merged[col].where(~merged[col].isna(), merged[f"{col}_tgn"])

    # Final clean: fill unknowns, stringify
    for c in merged.columns:
        merged[c] = merged[c].astype(str).where(~merged[c].isna(), "UNKNOWN").replace({"None":"UNKNOWN","nan":"UNKNOWN"})

    # Drop helper columns if present
    drop_cols = [c for c in merged.columns if c.endswith("_sp") or c.endswith("_tgn") or c == "player_id_tgn"]
    merged = merged.drop(columns=drop_cols, errors="ignore")

    # Save
    PROJ_OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(PROJ_OUT, index=False)

    # Simple progress info
    have_ctx = (merged["game_id"]!="UNKNOWN") & (merged["team_id"]!="UNKNOWN") & (merged["opponent_team_id"]!="UNKNOWN")
    print(f"Injected context for {have_ctx.sum()}/{len(merged)} pitchers -> {PROJ_OUT}")

if __name__ == "__main__":
    main()
