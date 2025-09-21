#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Robust prep with normalization + upstream de-duplication:
# - Resolves team_id from: existing -> lineups.csv -> normalized team text -> static MLB map.
# - Resolves game_id ONLY from todaysgames_normalized.csv (authoritative slate).
# - Logs and DROPS rows with unresolved team_id or with team_id but NO game_id (off-slate).
# - Normalizes team_id and game_id to clean string (no NaN, no float .0, no "0").
# - Enforces uniqueness on (player_id, game_id) for batter inputs; keeps highest proj_pa_used.
# - Writes concise logs + grouped dup diagnostics (not one line per row).
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

# Canonicalized name/aliases -> MLB abbrev
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

def normalize_id(val: str) -> str:
    """Force IDs to plain string, drop .0 floats, strip whitespace, never NaN."""
    s = str(val or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s

def _canon(s: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z]", "", str(s or "").lower())

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
        tgn[c] = tgn[c].apply(normalize_id)

    home = tgn.rename(columns={"home_team_id":"team_id"})[["game_id","team_id"]]
    away = tgn.rename(columns={"away_team_id":"team_id"})[["game_id","team_id"]]
    team_game = pd.concat([home, away], ignore_index=True).drop_duplicates()
    team_game = team_game[team_game["team_id"].str.len() > 0]

    a_home = tgn.rename(columns={"home_team":"abbrev","home_team_id":"team_id"})[["abbrev","team_id"]]
    a_away = tgn.rename(columns={"away_team":"abbrev","away_team_id":"team_id"})[["abbrev","team_id"]]
    abbrev_to_id_today = pd.concat([a_home, a_away], ignore_index=True).dropna().drop_duplicates()

    per_game = team_game.groupby("game_id")["team_id"].nunique()
    bad = per_game[per_game != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} has games without exactly two teams: {bad.to_dict()}")

    return team_game, abbrev_to_id_today

def resolve_team_id(row, abbrev_to_id_today: pd.DataFrame) -> str:
    # 1) existing
    if normalize_id(row.get("team_id")):
        return normalize_id(row.get("team_id"))
    # 2) from lineups (merged as team_id_lineups)
    if normalize_id(row.get("team_id_lineups")):
        return normalize_id(row.get("team_id_lineups"))
    # 3) from team text -> abbrev -> id (today) -> static fallback
    abbrev = normalize_to_abbrev(row.get("team") or "")
    if abbrev:
        m = abbrev_to_id_today.loc[abbrev_to_id_today["abbrev"] == abbrev, "team_id"]
        if not m.empty:
            return normalize_id(m.iloc[0])
        if abbrev in STATIC_ABBREV_TO_TEAM_ID:
            return str(STATIC_ABBREV_TO_TEAM_ID[abbrev])
    return ""  # unresolved

def dedupe_batter_inputs(df: pd.DataFrame, name_for_logs: str) -> tuple[pd.DataFrame, int]:
    """
    Enforce one row per (player_id, game_id):
    - Keep the row with largest proj_pa_used (numeric); if tie, keep first.
    - Write grouped counts to summaries/07_final/prep_dups_in_<name>.csv
    Returns (deduped_df, dropped_count)
    """
    if not {"player_id", "game_id"}.issubset(df.columns):
        return df, 0

    # Ensure numeric proj_pa_used exists for ranking
    if "proj_pa_used" not in df.columns:
        df["proj_pa_used"] = 0.0
    df["proj_pa_used_num"] = pd.to_numeric(df["proj_pa_used"], errors="coerce").fillna(0.0)

    # Find dup groups BEFORE dropping
    dup_mask = df.duplicated(subset=["player_id", "game_id"], keep=False)
    dup_groups = (
        df.loc[dup_mask, ["player_id", "game_id", "team_id", "proj_pa_used_num"]]
          .groupby(["player_id", "game_id", "team_id"], dropna=False)
          .agg(count=("proj_pa_used_num", "size"),
               kept_proj_pa_used=("proj_pa_used_num", "max"))
          .reset_index()
          .sort_values(["count", "kept_proj_pa_used"], ascending=[False, False])
    )
    if not dup_groups.empty:
        out = SUM_DIR / f"prep_dups_in_{name_for_logs}.csv"
        dup_groups.to_csv(out, index=False)

    before = len(df)
    # Keep max proj_pa_used per (player_id, game_id)
    df = (
        df.sort_values(["player_id", "game_id", "proj_pa_used_num"], ascending=[True, True, False])
          .drop_duplicates(subset=["player_id", "game_id"], keep="first")
          .drop(columns=["proj_pa_used_num"])
          .reset_index(drop=True)
    )
    dropped = before - len(df)
    return df, dropped

def inject_team_and_game(df: pd.DataFrame, name_for_logs: str,
                         lineups: pd.DataFrame,
                         team_game_map: pd.DataFrame,
                         abbrev_to_id_today: pd.DataFrame) -> tuple[pd.DataFrame,int,int,int,int]:
    """
    Returns (clean_df, dropped_off_slate, dropped_missing_team, dropped_missing_game_after_merge, dropped_dups)
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
    merged["team_id"] = merged["team_id"].apply(normalize_id)

    # attach game_id via slate map
    merged = merged.merge(team_game_map, on="team_id", how="left", suffixes=("", "_from_map"))
    existing_gid = merged["game_id"] if "game_id" in merged.columns else pd.Series([""]*len(merged))
    from_map     = merged["game_id_from_map"] if "game_id_from_map" in merged.columns else pd.Series([""]*len(merged))
    merged["game_id"] = existing_gid.where(existing_gid.astype(str).str.len() > 0,
                                           from_map.astype(str).str.strip())
    if "game_id_from_map" in merged.columns:
        merged.drop(columns=["game_id_from_map"], inplace=True)
    merged["game_id"] = merged["game_id"].apply(normalize_id)

    # off-slate: have team_id but no game_id -> drop
    off_slate = merged[(merged["team_id"].str.len() > 0) & (merged["game_id"].str.len() == 0)]
    dropped_off = len(off_slate)
    if dropped_off:
        off_slate[["player_id","team","team_id"]].drop_duplicates().to_csv(
            SUM_DIR / f"off_slate_dropped_in_{name_for_logs}.csv", index=False)
        merged = merged.drop(off_slate.index)

    # still-missing team_id -> drop
    miss_team = merged[merged["team_id"].str.len() == 0]
    dropped_team = len(miss_team)
    if dropped_team:
        miss_team[["player_id","team"]].drop_duplicates().to_csv(
            SUM_DIR / f"missing_team_id_in_{name_for_logs}.csv", index=False)
        merged = merged.drop(miss_team.index)

    # still-missing game_id -> drop
    miss_gid = merged[merged["game_id"].str.len() == 0]
    dropped_gid = len(miss_gid)
    if dropped_gid:
        miss_gid[["player_id","team","team_id"]].drop_duplicates().to_csv(
            SUM_DIR / f"missing_game_id_in_{name_for_logs}.csv", index=False)
        merged = merged.drop(miss_gid.index)

    # Upstream de-dup on (player_id, game_id)
    merged_deduped, dropped_dups = dedupe_batter_inputs(merged, name_for_logs)

    kept = len(merged_deduped)
    log(f"[INFO] {name_for_logs}: start={start_rows}, kept={kept}, "
        f"dropped_off_slate={dropped_off}, dropped_missing_team_id={dropped_team}, "
        f"dropped_missing_game_id={dropped_gid}, dropped_duplicate_keys={dropped_dups}")

    # drop helper columns
    if "team_id_lineups" in merged_deduped.columns:
        merged_deduped.drop(columns=["team_id_lineups"], inplace=True)

    return merged_deduped, dropped_off, dropped_team, dropped_gid, dropped_dups

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

    bp_out, bp_off, bp_mteam, bp_mgid, bp_dups = inject_team_and_game(
        bat_proj, "batter_props_projected_final.csv", lineups, team_game_map, abbrev_to_id_today
    )
    bx_out, bx_off, bx_mteam, bx_mgid, bx_dups = inject_team_and_game(
        bat_exp,  "batter_props_expanded_final.csv",  lineups, team_game_map, abbrev_to_id_today
    )

    write_back(bat_proj, bp_out, BATTERS_PROJECTED)
    write_back(bat_exp,  bx_out,  BATTERS_EXPANDED)

    # summary line(s) for CI step output
    log(f"[INFO] batter_props_projected_final.csv: kept={len(bp_out)}, "
        f"dropped_off_slate={bp_off}, dropped_missing_team_id={bp_mteam}, "
        f"dropped_missing_game_id={bp_mgid}, dropped_duplicate_keys={bp_dups}")
    log(f"[INFO] batter_props_expanded_final.csv: kept={len(bx_out)}, "
        f"dropped_off_slate={bx_off}, dropped_missing_team_id={bx_mteam}, "
        f"dropped_missing_game_id={bx_mgid}, dropped_duplicate_keys={bx_dups}")
    log("OK: wrote data/_projections/batter_props_projected_final.csv and data/_projections/batter_props_expanded_final.csv")

if __name__ == "__main__":
    main()
