#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Robust prep:
# - Resolves team_id from: existing -> lineups.csv -> normalized team text -> static MLB map.
# - Resolves game_id ONLY from todaysgames_normalized.csv (authoritative slate).
# - Logs and DROPS rows with unresolved team_id or with team_id but NO game_id (off-slate).
# - Never writes literal "0" placeholders.
# - Writes a concise prep log with counts.
#
from __future__ import annotations

import re
import pandas as pd
from pathlib import Path

PROJ_DIR = Path("data/_projections")
RAW_DIR  = Path("data/raw")
SUM_DIR  = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_PROJECTED = PROJ_DIR / "batter_props_projected_final.csv"
BATTERS_EXPANDED  = PROJ_DIR / "batter_props_expanded_final.csv"
LINEUPS_CSV       = RAW_DIR / "lineups.csv"
TGN_CSV           = RAW_DIR / "todaysgames_normalized.csv"

LOG_FILE = SUM_DIR / "prep_daily_log.txt"

# Static MLB abbrev -> team_id (covers off-slate resolution)
STATIC_ABBREV_TO_TEAM_ID = {
    # AL East
    "BAL": 110, "BOS": 111, "NYY": 147, "TB": 139, "TOR": 141,
    # AL Central
    "CWS": 145, "CLE": 114, "DET": 116, "KC": 118, "MIN": 142,
    # AL West
    "HOU": 117, "LAA": 108, "ATH": 133, "SEA": 136, "TEX": 140,
    # NL East
    "ATL": 144, "MIA": 146, "NYM": 121, "PHI": 143, "WSH": 120,
    # NL Central
    "CHC": 112, "CIN": 113, "MIL": 158, "PIT": 134, "STL": 138,
    # NL West
    "ARI": 109, "COL": 115, "LAD": 119, "SD": 135, "SF": 137,
}

TEAM_ALIASES_TO_ABBREV = {
    "angels":"LAA","laa":"LAA","losangelesangels":"LAA","laangels":"LAA",
    "athletics":"ATH","ath":"ATH","oakland":"ATH","oak":"ATH",
    "bluejays":"TOR","jays":"TOR","toronto":"TOR","tor":"TOR",
    "orioles":"BAL","bal":"BAL","baltimore":"BAL",
    "rays":"TB","ray":"TB","tampabay":"TB","tampa":"TB","tb":"TB",
    "redsox":"BOS","bos":"BOS","boston":"BOS",
    "yankees":"NYY","nyy":"NYY","newyorkyankees":"NYY",
    "guardians":"CLE","indians":"CLE","cle":"CLE","cleveland":"CLE",
    "tigers":"DET","det":"DET","detroit":"DET",
    "twins":"MIN","min":"MIN","minnesota":"MIN",
    "whitesox":"CWS","cws":"CWS","chicagowhitesox":"CWS",
    "royals":"KC","kcr":"KC","kc":"KC","kansascity":"KC",
    "mariners":"SEA","sea":"SEA","seattle":"SEA",
    "astros":"HOU","hou":"HOU","houston":"HOU",
    "rangers":"TEX","tex":"TEX","texas":"TEX",
    "braves":"ATL","atl":"ATL","atlanta":"ATL",
    "marlins":"MIA","mia":"MIA","miami":"MIA",
    "mets":"NYM","nym":"NYM","newyorkmets":"NYM",
    "phillies":"PHI","phi":"PHI","philadelphia":"PHI",
    "nationals":"WSH","was":"WSH","wsh":"WSH","washington":"WSH",
    "cubs":"CHC","chc":"CHC","chicagocubs":"CHC",
    "reds":"CIN","cin":"CIN","cincinnati":"CIN",
    "brewers":"MIL","mil":"MIL","milwaukee":"MIL",
    "pirates":"PIT","pit":"PIT","pittsburgh":"PIT",
    "cardinals":"STL","stl":"STL","stlouis":"STL","saintlouis":"STL",
    "diamondbacks":"ARI","dbacks":"ARI","d-backs":"ARI","ari":"ARI","arizona":"ARI",
    "rockies":"COL","col":"COL","colorado":"COL",
    "dodgers":"LAD","lad":"LAD","losangelesdodgers":"LAD","ladodgers":"LAD",
    "giants":"SF","sfg":"SF","sf":"SF","sanfrancisco":"SF",
    "padres":"SD","sd":"SD","sandiego":"SD",
}

def log(msg: str) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg, flush=True)

def read_csv_force_str(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().replace({"None":"","nan":"","NaN":""})
    return df

def _canon(s: str) -> str:
    return re.sub(r"[^a-z]", "", str(s or "").lower())

def normalize_to_abbrev(team_text: str) -> str:
    t = _canon(team_text)
    if not t:
        return ""
    if t in TEAM_ALIASES_TO_ABBREV:
        return TEAM_ALIASES_TO_ABBREV[t]
    up = str(team_text or "").strip().upper()
    if up in STATIC_ABBREV_TO_TEAM_ID:
        return up
    return ""

def build_team_maps_from_tgn(tgn: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    need = {"game_id","home_team_id","away_team_id","home_team","away_team"}
    miss = sorted(list(need - set(tgn.columns)))
    if miss:
        raise RuntimeError(f"{TGN_CSV} missing columns: {miss}")

    tgn = tgn[["game_id","home_team_id","away_team_id","home_team","away_team"]].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()

    home = tgn.rename(columns={"home_team_id":"team_id"})[["game_id","team_id"]]
    away = tgn.rename(columns={"away_team_id":"team_id"})[["game_id","team_id"]]
    team_game = pd.concat([home, away], ignore_index=True).drop_duplicates()
    team_game["team_id"] = team_game["team_id"].replace({"None":"","nan":"","NaN":""})
    team_game = team_game[team_game["team_id"].str.len() > 0]

    a_home = tgn.rename(columns={"home_team":"abbrev","home_team_id":"team_id"})[["abbrev","team_id"]]
    a_away = tgn.rename(columns={"away_team":"abbrev","away_team_id":"team_id"})[["abbrev","team_id"]]
    abbrev_to_id_today = pd.concat([a_home, a_away], ignore_index=True).dropna().drop_duplicates()

    # sanity: exactly two teams per game
    per_game = team_game.groupby("game_id")["team_id"].nunique()
    bad = per_game[per_game != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} has games without exactly two teams: {bad.to_dict()}")

    return team_game, abbrev_to_id_today

def resolve_team_id(row, abbrev_to_id_today: pd.DataFrame) -> str:
    # 1) existing
    if str(row.get("team_id") or "").strip():
        return str(row["team_id"]).strip()
    # 2) from lineups (merged as team_id_lineups)
    if str(row.get("team_id_lineups") or "").strip():
        return str(row["team_id_lineups"]).strip()
    # 3) from team text -> abbrev -> id (today) -> static fallback
    abbrev = normalize_to_abbrev(row.get("team") or "")
    if abbrev:
        m = abbrev_to_id_today.loc[abbrev_to_id_today["abbrev"] == abbrev, "team_id"]
        if not m.empty:
            return str(m.iloc[0]).strip()
        if abbrev in STATIC_ABBREV_TO_TEAM_ID:
            return str(STATIC_ABBREV_TO_TEAM_ID[abbrev])
    return ""  # unresolved

def inject_team_and_game(df: pd.DataFrame, name_for_logs: str,
                         lineups: pd.DataFrame,
                         team_game_map: pd.DataFrame,
                         abbrev_to_id_today: pd.DataFrame) -> tuple[pd.DataFrame,int,int,int]:
    """
    Returns (clean_df, dropped_off_slate, dropped_missing_team, dropped_missing_game_after_merge)
    """
    start_rows = len(df)
    if "player_id" not in df.columns:
        raise RuntimeError(f"{name_for_logs} missing required column: player_id")

    # normalize strings
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # attach lineups helper
    li = lineups.rename(columns={"team_id":"team_id_lineups"})[["player_id","team_id_lineups"]].copy()
    merged = df.merge(li, on="player_id", how="left")

    # resolve team_id robustly
    merged["team_id"] = merged.apply(lambda r: resolve_team_id(r, abbrev_to_id_today), axis=1)

    # attach game_id via slate map
    merged = merged.merge(team_game_map, on="team_id", how="left", suffixes=("", "_from_map"))
    existing_gid = merged["game_id"] if "game_id" in merged.columns else pd.Series([""]*len(merged))
    from_map     = merged["game_id_from_map"] if "game_id_from_map" in merged.columns else pd.Series([""]*len(merged))
    merged["game_id"] = existing_gid.where(existing_gid.astype(str).str.len() > 0,
                                           from_map.astype(str).str.strip())
    if "game_id_from_map" in merged.columns:
        merged.drop(columns=["game_id_from_map"], inplace=True)

    # off-slate: have team_id but no game_id -> drop
    off_slate = merged[(merged["team_id"].astype(str).str.len() > 0) &
                       (merged["game_id"].astype(str).str.len() == 0)]
    dropped_off = len(off_slate)
    if dropped_off:
        off_slate[["player_id","team","team_id"]].drop_duplicates() \
            .to_csv(SUM_DIR / f"off_slate_dropped_in_{name_for_logs}.csv", index=False)
        merged = merged[~merged.index.isin(off_slate.index)].copy()

    # still-missing team_id -> drop
    miss_team = merged[merged["team_id"].astype(str).str.len() == 0]
    dropped_team = len(miss_team)
    if dropped_team:
        miss_team[["player_id","team"]].drop_duplicates() \
            .to_csv(SUM_DIR / f"missing_team_id_in_{name_for_logs}.csv", index=False)
        merged = merged[~merged.index.isin(miss_team.index)].copy()

    # still-missing game_id -> drop
    miss_gid = merged[merged["game_id"].astype(str).str.len() == 0]
    dropped_gid = len(miss_gid)
    if dropped_gid:
        miss_gid[["player_id","team","team_id"]].drop_duplicates() \
            .to_csv(SUM_DIR / f"missing_game_id_in_{name_for_logs}.csv", index=False)
        merged = merged[~merged.index.isin(miss_gid.index)].copy()

    kept = len(merged)
    log(f"[INFO] {name_for_logs}: start={start_rows}, kept={kept}, "
        f"dropped_off_slate={dropped_off}, dropped_missing_team_id={dropped_team}, "
        f"dropped_missing_game_id={dropped_gid}")

    # drop helper columns
    for c in ["team_id_lineups"]:
        if c in merged.columns:
            merged.drop(columns=[c], inplace=True)

    return merged, dropped_off, dropped_team, dropped_gid

def write_back(df_before: pd.DataFrame, df_after: pd.DataFrame, path: Path) -> None:
    cols = list(df_before.columns)
    for add_col in ["team_id","game_id"]:
        if add_col not in cols:
            cols.append(add_col)
    cols_final = [c for c in cols if c in df_after.columns]
    df_after[cols_final].to_csv(path, index=False)

def main() -> None:
    # fresh log
    LOG_FILE.write_text("", encoding="utf-8")
    log("PREP: injecting team_id and game_id into batter *_final.csv (drop unresolved/off-slate)")

    bat_proj = read_csv_force_str(BATTERS_PROJECTED)
    bat_exp  = read_csv_force_str(BATTERS_EXPANDED)
    lineups  = read_csv_force_str(LINEUPS_CSV)
    tgn      = read_csv_force_str(TGN_CSV)

    team_game_map, abbrev_to_id_today = build_team_maps_from_tgn(tgn)

    bp_out, bp_off, bp_mteam, bp_mgid = inject_team_and_game(
        bat_proj, "batter_props_projected_final.csv", lineups, team_game_map, abbrev_to_id_today
    )
    bx_out, bx_off, bx_mteam, bx_mgid = inject_team_and_game(
        bat_exp,  "batter_props_expanded_final.csv",  lineups, team_game_map, abbrev_to_id_today
    )

    write_back(bat_proj, bp_out, BATTERS_PROJECTED)
    write_back(bat_exp,  bx_out,  BATTERS_EXPANDED)

    # summary line(s) for CI step output
    log(f"[INFO] batter_props_projected_final.csv: kept={len(bp_out)}, "
        f"dropped_off_slate={bp_off}, dropped_missing_team_id={bp_mteam}, dropped_missing_game_id={bp_mgid}")
    log(f"[INFO] batter_props_expanded_final.csv: kept={len(bx_out)}, "
        f"dropped_off_slate={bx_off}, dropped_missing_team_id={bx_mteam}, dropped_missing_game_id={bx_mgid}")
    log("OK: wrote data/_projections/batter_props_projected_final.csv and data/_projections/batter_props_expanded_final.csv")

if __name__ == "__main__":
    main()
