#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# What this does (robust version):
# - Ensures every batter row ends up with a valid team_id and (if on today's slate) a game_id.
# - team_id resolution order:
#       existing -> lineups.csv -> normalized team text -> STATIC_ABBREV_TO_TEAM_ID
# - game_id comes ONLY from todaysgames_normalized.csv (authoritative slate).
# - Rows that have a team_id but NO game_id (off-slate players/teams) are LOGGED and DROPPED.
# - Writes diagnostics to summaries/07_final/ and fails fast on truly unmapped cases.
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

LOG_FILE = SUM_DIR / "prep_injection_log.txt"

# Static MLB abbrev -> team_id map (covers off-slate teams)
# (Filled with standard MLB IDs; add/adjust if your repo uses different IDs.)
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

# Normalize team names/nicknames to abbreviations used in TGN + static map
TEAM_ALIASES_TO_ABBREV = {
    # Angels
    "angels": "LAA", "laa": "LAA", "losangelesangels": "LAA", "laangels": "LAA",
    # Athletics (note: your TGN uses 'ATH' not 'OAK')
    "athletics": "ATH", "ath": "ATH", "oakland": "ATH", "oak": "ATH",
    # Blue Jays
    "bluejays": "TOR", "jays": "TOR", "toronto": "TOR", "tor": "TOR",
    # Orioles
    "orioles": "BAL", "bal": "BAL", "baltimore": "BAL",
    # Rays
    "rays": "TB", "ray": "TB", "tampabay": "TB", "tampa": "TB", "tb": "TB",
    # Red Sox
    "redsox": "BOS", "bos": "BOS", "boston": "BOS",
    # Yankees
    "yankees": "NYY", "nyy": "NYY", "newyorkyankees": "NYY",
    # Guardians
    "guardians": "CLE", "indians": "CLE", "cle": "CLE", "cleveland": "CLE",
    # Tigers
    "tigers": "DET", "det": "DET", "detroit": "DET",
    # Twins
    "twins": "MIN", "min": "MIN", "minnesota": "MIN",
    # White Sox
    "whitesox": "CWS", "cws": "CWS", "chicagowhitesox": "CWS",
    # Royals
    "royals": "KC", "kcr": "KC", "kc": "KC", "kansascity": "KC",
    # Mariners
    "mariners": "SEA", "sea": "SEA", "seattle": "SEA",
    # Astros
    "astros": "HOU", "hou": "HOU", "houston": "HOU",
    # Rangers
    "rangers": "TEX", "tex": "TEX", "texas": "TEX",
    # Braves
    "braves": "ATL", "atl": "ATL", "atlanta": "ATL",
    # Marlins
    "marlins": "MIA", "mia": "MIA", "miami": "MIA",
    # Mets
    "mets": "NYM", "nym": "NYM", "newyorkmets": "NYM",
    # Phillies
    "phillies": "PHI", "phi": "PHI", "philadelphia": "PHI",
    # Nationals
    "nationals": "WSH", "was": "WSH", "wsh": "WSH", "washington": "WSH",
    # Cubs
    "cubs": "CHC", "chc": "CHC", "chicagocubs": "CHC",
    # Reds
    "reds": "CIN", "cin": "CIN", "cincinnati": "CIN",
    # Brewers
    "brewers": "MIL", "mil": "MIL", "milwaukee": "MIL",
    # Pirates
    "pirates": "PIT", "pit": "PIT", "pittsburgh": "PIT",
    # Cardinals
    "cardinals": "STL", "stl": "STL", "stlouis": "STL", "saintlouis": "STL",
    # Diamondbacks
    "diamondbacks": "ARI", "dbacks": "ARI", "d-backs": "ARI", "ari": "ARI", "arizona": "ARI",
    # Rockies
    "rockies": "COL", "col": "COL", "colorado": "COL",
    # Dodgers
    "dodgers": "LAD", "lad": "LAD", "losangelesdodgers": "LAD", "ladodgers": "LAD",
    # Giants (your TGN uses 'SF')
    "giants": "SF", "sfg": "SF", "sf": "SF", "sanfrancisco": "SF",
    # Padres
    "padres": "SD", "sd": "SD", "sandiego": "SD",
}

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

def _canon(s: str) -> str:
    s = str(s or "").lower()
    return re.sub(r"[^a-z]", "", s)

def normalize_to_abbrev(team_text: str) -> str:
    t = _canon(team_text)
    if not t:
        return ""
    if t in TEAM_ALIASES_TO_ABBREV:
        return TEAM_ALIASES_TO_ABBREV[t]
    # pass-through if already an abbrev seen in TGN/static map
    up = str(team_text or "").strip().upper()
    if up in STATIC_ABBREV_TO_TEAM_ID:
        return up
    return ""

def build_team_maps_from_tgn(tgn: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    need = {"game_id", "home_team_id", "away_team_id", "home_team", "away_team"}
    miss = sorted(list(need - set(tgn.columns)))
    if miss:
        raise RuntimeError(f"{TGN_CSV} missing columns: {miss}")

    tgn = tgn[["game_id","home_team_id","away_team_id","home_team","away_team"]].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()

    # (team_id -> game_id) exploded
    home = tgn.rename(columns={"home_team_id":"team_id"})[["game_id","team_id"]]
    away = tgn.rename(columns={"away_team_id":"team_id"})[["game_id","team_id"]]
    team_game = pd.concat([home, away], ignore_index=True).drop_duplicates()
    team_game["team_id"] = team_game["team_id"].replace({"None": "", "nan": "", "NaN": ""})
    team_game = team_game[team_game["team_id"].str.len() > 0]

    # Validate: exactly two teams per game
    per_game = team_game.groupby("game_id")["team_id"].nunique()
    bad = per_game[per_game != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} has games without exactly two teams: {bad.to_dict()}")

    # Build abbrev -> team_id from both sides (today's slate only)
    a_home = tgn.rename(columns={"home_team":"abbrev", "home_team_id":"team_id"})[["abbrev","team_id"]]
    a_away = tgn.rename(columns={"away_team":"abbrev", "away_team_id":"team_id"})[["abbrev","team_id"]]
    abbrev_to_id_today = pd.concat([a_home, a_away], ignore_index=True).dropna().drop_duplicates()

    return team_game, abbrev_to_id_today

def resolve_team_id(row, abbrev_to_id_today: pd.DataFrame) -> str:
    """
    Resolve a single row's team_id using:
      existing -> lineups -> normalized team text -> today's abbrev map -> STATIC_ABBREV_TO_TEAM_ID
    """
    # 1) existing
    if str(row.get("team_id") or "").strip():
        return str(row["team_id"]).strip()

    # 2) from lineups (already merged as 'team_id_lineups')
    if str(row.get("team_id_lineups") or "").strip():
        return str(row["team_id_lineups"]).strip()

    # 3) normalized team text -> abbrev
    team_txt = str(row.get("team") or "")
    abbrev = normalize_to_abbrev(team_txt)

    # 4) today's abbrev map
    if abbrev:
        m = abbrev_to_id_today.loc[abbrev_to_id_today["abbrev"] == abbrev, "team_id"]
        if not m.empty:
            return str(m.iloc[0]).strip()

    # 5) static map (off-slate fallback)
    if abbrev and abbrev in STATIC_ABBREV_TO_TEAM_ID:
        return str(STATIC_ABBREV_TO_TEAM_ID[abbrev])

    return ""  # unresolved

def inject_team_and_game(df: pd.DataFrame, name_for_logs: str,
                         lineups: pd.DataFrame,
                         team_game_map: pd.DataFrame,
                         abbrev_to_id_today: pd.DataFrame) -> pd.DataFrame:
    if "player_id" not in df.columns:
        raise RuntimeError(f"{name_for_logs} missing required column: player_id")

    # Normalize string dtypes
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Attach team_id from lineups (as helper)
    li = lineups.rename(columns={"team_id":"team_id_lineups"})[["player_id","team_id_lineups"]].copy()
    merged = df.merge(li, on="player_id", how="left")

    # Resolve team_id per row with robust logic
    merged["team_id"] = merged.apply(lambda r: resolve_team_id(r, abbrev_to_id_today), axis=1)

    # Attach game_id via authoritative slate map (team_id -> game_id)
    merged = merged.merge(team_game_map, on="team_id", how="left", suffixes=("", "_from_map"))
    # Prefer existing game_id if present; else from map
    existing_gid = merged["game_id"] if "game_id" in merged.columns else pd.Series([""]*len(merged))
    from_map     = merged["game_id_from_map"] if "game_id_from_map" in merged.columns else pd.Series([""]*len(merged))
    merged["game_id"] = existing_gid.where(existing_gid.astype(str).str.len() > 0, from_map.astype(str).str.strip())
    if "game_id_from_map" in merged.columns:
        merged.drop(columns=["game_id_from_map"], inplace=True)

    # Split: off-slate rows (have team_id but NO game_id) — log & drop
    off_slate = merged[(merged["team_id"].astype(str).str.len() > 0) & (merged["game_id"].astype(str).str.len() == 0)]
    if len(off_slate) > 0:
        out = SUM_DIR / f"off_slate_dropped_in_{name_for_logs}.csv"
        off_slate[["player_id","team","team_id"]].drop_duplicates().to_csv(out, index=False)
        log(f"[INFO] {name_for_logs}: dropped {len(off_slate)} off-slate rows (no game_id). See {out}")
        merged = merged[~merged.index.isin(off_slate.index)].copy()

    # Diagnostics for any remaining problems
    miss_team = merged.loc[merged["team_id"].astype(str).str.len() == 0,
                           ["player_id"] + (["team"] if "team" in merged.columns else [])].drop_duplicates()
    miss_gid  = merged.loc[merged["game_id"].astype(str).str.len() == 0,
                           ["player_id","team_id"]].drop_duplicates()

    if len(miss_team) > 0:
        out = SUM_DIR / f"missing_team_id_in_{name_for_logs}.csv"
        miss_team.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_team)} rows still missing team_id ({out})")

    if len(miss_gid) > 0:
        out = SUM_DIR / f"missing_game_id_in_{name_for_logs}.csv"
        miss_gid.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_gid)} rows still missing game_id ({out})")

    log(f"[INFO] {name_for_logs}: kept_rows={len(merged)} missing_team_id={len(miss_team)} missing_game_id={len(miss_gid)}")
    return merged

def write_back(df_before: pd.DataFrame, df_after: pd.DataFrame, path: Path) -> None:
    # Preserve original column order; append team_id/game_id at end if new.
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

    team_game_map, abbrev_to_id_today = build_team_maps_from_tgn(tgn)

    bat_proj_out = inject_team_and_game(bat_proj, "batter_props_projected_final.csv",
                                        lineups, team_game_map, abbrev_to_id_today)
    bat_exp_out  = inject_team_and_game(bat_exp,  "batter_props_expanded_final.csv",
                                        lineups, team_game_map, abbrev_to_id_today)

    # Hard fail only if *after dropping off-slate* we still have unresolved keys
    err = []
    if (bat_proj_out["team_id"].astype(str).str.len() == 0).any():
        err.append("projected: team_id")
    if (bat_proj_out["game_id"].astype(str).str.len() == 0).any():
        err.append("projected: game_id")
    if (bat_exp_out["team_id"].astype(str).str.len() == 0).any():
        err.append("expanded: team_id")
    if (bat_exp_out["game_id"].astype(str).str.len() == 0).any():
        err.append("expanded: game_id")
    if err:
        raise RuntimeError("prepare_daily_projection_inputs: unresolved keys -> " + ", ".join(err))

    write_back(bat_proj, bat_proj_out, BATTERS_PROJECTED)
    write_back(bat_exp,  bat_exp_out,  BATTERS_EXPANDED)

    log(f"OK: wrote {BATTERS_PROJECTED} and {BATTERS_EXPANDED}")

if __name__ == "__main__":
    main()
