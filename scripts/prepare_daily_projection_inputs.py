#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Fixes:
# - Stronger coalesce & validation for team_id/game_id.
# - Canonical team->game map from todaysgames_normalized.csv.
# - Emits explicit diagnostics and FAILS if any batter rows remain without game_id.
# - Verifies every mapped (team_id, game_id) exists in the slate (2 teams per game).
#
from __future__ import annotations

import sys
import pandas as pd
from pathlib import Path

# Paths
PROJ_DIR = Path("data/_projections")
RAW_DIR = Path("data/raw")
SUM_DIR = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_PROJECTED = PROJ_DIR / "batter_props_projected_final.csv"
BATTERS_EXPANDED  = PROJ_DIR / "batter_props_expanded_final.csv"
LINEUPS_CSV       = RAW_DIR / "lineups.csv"
TGN_CSV           = RAW_DIR / "todaysgames_normalized.csv"

LOG_FILE = SUM_DIR / "prep_injection_log.txt"

def log(msg: str) -> None:
    print(msg, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")

def read_csv_force_str(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
        df[c] = df[c].replace({"None": "", "nan": "", "NaN": ""})
    return df

def coalesce_series(a: pd.Series | None, b: pd.Series | None) -> pd.Series:
    if a is None and b is None:
        return pd.Series([], dtype="object")
    if a is None:
        a = pd.Series([""] * len(b), index=b.index, dtype="object")
    if b is None:
        b = pd.Series([""] * len(a), index=a.index, dtype="object")
    a = a.astype(str)
    b = b.astype(str)
    out = a.where(a.str.len() > 0, b)
    return out.fillna("").astype(str)

def build_team_to_game_map(tgn: pd.DataFrame) -> pd.DataFrame:
    need = {"game_id", "home_team_id", "away_team_id"}
    missing = sorted(list(need - set(tgn.columns)))
    if missing:
        raise RuntimeError(f"{TGN_CSV} missing columns: {missing}")

    tgn = tgn[["game_id", "home_team_id", "away_team_id"]].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()

    home = tgn.rename(columns={"home_team_id": "team_id"})[["game_id", "team_id"]].copy()
    away = tgn.rename(columns={"away_team_id": "team_id"})[["game_id", "team_id"]].copy()
    team_game = pd.concat([home, away], ignore_index=True)

    team_game["team_id"] = team_game["team_id"].replace({"None": "", "nan": "", "NaN": ""})
    team_game = team_game[team_game["team_id"].astype(str).str.len() > 0].drop_duplicates(ignore_index=True)

    # Validate: each game appears exactly twice (two teams)
    per_game = team_game.groupby("game_id")["team_id"].nunique()
    bad = per_game[per_game != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} has games without exactly two teams: {bad.to_dict()}")

    return team_game

def inject_team_and_game(df: pd.DataFrame, name_for_logs: str,
                         lineups: pd.DataFrame, team_game_map: pd.DataFrame) -> pd.DataFrame:
    if "player_id" not in df.columns:
        raise RuntimeError(f"{name_for_logs} missing required column: player_id")

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Bring team_id from lineups
    li = lineups.rename(columns={"team_id": "team_id_lineups"})[["player_id", "team_id_lineups"]].copy()
    merged = df.merge(li, on="player_id", how="left")

    # Coalesce team_id
    existing_team = merged["team_id"] if "team_id" in merged.columns else None
    from_lineups  = merged["team_id_lineups"] if "team_id_lineups" in merged.columns else None
    merged["team_id"] = coalesce_series(existing_team, from_lineups).astype(str)

    # Attach game_id via canonical mapping (coalesce with any pre-existing)
    merged = merged.merge(team_game_map, on="team_id", how="left", suffixes=("", "_from_map"))
    existing_gid = merged["game_id"] if "game_id" in merged.columns else None
    from_map     = merged["game_id_from_map"] if "game_id_from_map" in merged.columns else None
    merged["game_id"] = coalesce_series(existing_gid, from_map)
    if "game_id_from_map" in merged.columns:
        merged.drop(columns=["game_id_from_map"], inplace=True)

    # Diagnostics
    miss_team = merged.loc[merged["team_id"].astype(str).str.len() == 0, ["player_id"]].drop_duplicates()
    miss_gid  = merged.loc[merged["game_id"].astype(str).str.len() == 0, ["player_id", "team_id"]].drop_duplicates()

    if len(miss_team) > 0:
        out = SUM_DIR / f"missing_team_id_in_{name_for_logs}.csv"
        miss_team.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_team)} rows missing team_id ({out})")

    if len(miss_gid) > 0:
        out = SUM_DIR / f"missing_game_id_in_{name_for_logs}.csv"
        miss_gid.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_gid)} rows missing game_id ({out})")

    log(f"[INFO] {name_for_logs}: missing team_id={len(miss_team)}, missing game_id={len(miss_gid)}")

    return merged

def write_back(df_before: pd.DataFrame, df_after: pd.DataFrame, path: Path) -> None:
    cols = list(df_before.columns)
    for add_col in ["team_id", "game_id"]:
        if add_col not in cols:
            cols.append(add_col)
    cols_final = [c for c in cols if c in df_after.columns]
    df_after[cols_final].to_csv(path, index=False)

def main() -> None:
    LOG_FILE.write_text("", encoding="utf-8")
    log("PREP: injecting team_id and game_id into batter *_final.csv")

    bat_proj = read_csv_force_str(BATTERS_PROJECTED)
    bat_exp  = read_csv_force_str(BATTERS_EXPANDED)
    lineups  = read_csv_force_str(LINEUPS_CSV)
    tgn      = read_csv_force_str(TGN_CSV)

    team_game_map = build_team_to_game_map(tgn)

    bat_proj_out = inject_team_and_game(bat_proj, "batter_props_projected_final.csv", lineups, team_game_map)
    bat_exp_out  = inject_team_and_game(bat_exp,  "batter_props_expanded_final.csv",  lineups, team_game_map)

    # Hard stop if any game_id still missing (prevents silent fallbacks later)
    if (bat_proj_out["game_id"].astype(str).str.len() == 0).any():
        raise RuntimeError("prepare_daily_projection_inputs: projected file has missing game_id after mapping.")
    if (bat_exp_out["game_id"].astype(str).str.len() == 0).any():
        raise RuntimeError("prepare_daily_projection_inputs: expanded file has missing game_id after mapping.")

    write_back(bat_proj, bat_proj_out, BATTERS_PROJECTED)
    write_back(bat_exp,  bat_exp_out,  BATTERS_EXPANDED)

    log(f"OK: wrote {BATTERS_PROJECTED} and {BATTERS_EXPANDED}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        msg = f"[ERROR] prepare_daily_projection_inputs failed: {repr(e)}"
        print(msg)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
        raise
