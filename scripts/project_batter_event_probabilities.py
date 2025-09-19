#!/usr/bin/env python3

import re
import pandas as pd
import numpy as np
from pathlib import Path

# Paths
DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
RAW_DIR    = Path("data/raw")
SUM_DIR    = Path("summaries/07_final")
OUT_DIR    = Path("data/_projections")
END_DIR    = Path("data/end_chain/final")
SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
END_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"
TGN_CSV         = RAW_DIR / "todaysgames_normalized.csv"

OUT_FILE_PROJ   = OUT_DIR / "batter_event_probabilities.csv"
OUT_FILE_FINAL  = END_DIR / "batter_event_probabilities.csv"

ADJ_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

# --- Static helpers (mirrors prep; includes off-slate fallback) ---------------

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

def _canon(s: str) -> str:
    s = str(s or "").lower()
    return re.sub(r"[^a-z]", "", s)

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

# --- Utility -----------------------------------------------------------------

def require(df, cols, name):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing columns: {miss}")

def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def safe_rate(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce").replace(0, np.nan)
    return (n / d).fillna(0.0).clip(0.0)

def log5(b, p, lg):
    if lg <= 0:
        return pd.Series(0.0, index=b.index)
    b = pd.to_numeric(b, errors="coerce").fillna(0.0)
    p = pd.to_numeric(p, errors="coerce").fillna(0.0)
    return (b * p) / lg

def build_team_to_game_map(tgn: pd.DataFrame) -> pd.DataFrame:
    need = {"game_id", "home_team_id", "away_team_id"}
    missing = sorted(list(need - set(tgn.columns)))
    if missing:
        raise RuntimeError(f"{TGN_CSV} missing columns: {missing}")
    tgn = tgn[["game_id", "home_team_id", "away_team_id"]].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()
    home = tgn.rename(columns={"home_team_id": "team_id"})[["game_id", "team_id"]]
    away = tgn.rename(columns={"away_team_id": "team_id"})[["game_id", "team_id"]]
    team_game = pd.concat([home, away], ignore_index=True).drop_duplicates()
    pg = team_game.groupby("game_id")["team_id"].nunique()
    bad = pg[pg != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} invalid two-team constraint: {bad.to_dict()}")
    return team_game

# --- Self-heal & drop unresolved ---------------------------------------------

def heal_and_filter_keys(bat_d: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Resolve missing team_id/game_id using team names and TGN.
    Rows still unresolved after healing are DROPPED (logged).
    Returns (patched_df, dropped_count).
    """
    dropped = 0

    # Ensure key columns exist and are strings
    for c in ["team_id","game_id","team"]:
        if c not in bat_d.columns:
            bat_d[c] = ""
        bat_d[c] = bat_d[c].astype(str)

    # Read TGN and build maps
    tgn = pd.read_csv(TGN_CSV, dtype=str)
    team_game = build_team_to_game_map(tgn)
    a_home = tgn.rename(columns={"home_team":"abbrev", "home_team_id":"team_id"})[["abbrev","team_id"]]
    a_away = tgn.rename(columns={"away_team":"abbrev", "away_team_id":"team_id"})[["abbrev","team_id"]]
    abbrev_to_id_today = pd.concat([a_home, a_away], ignore_index=True).dropna().drop_duplicates()

    # Fill team_id from team text where empty
    mask_missing_tid = bat_d["team_id"].eq("") | bat_d["team_id"].isna()
    if "team" in bat_d.columns and mask_missing_tid.any():
        tmp = bat_d.loc[mask_missing_tid, ["player_id","team"]].copy()
        tmp["abbrev"] = tmp["team"].apply(normalize_to_abbrev)
        tmp = tmp.merge(abbrev_to_id_today, how="left", on="abbrev")
        need_static = tmp["team_id"].isna() | tmp["team_id"].eq("")
        tmp.loc[need_static, "team_id"] = tmp.loc[need_static, "abbrev"].map(STATIC_ABBREV_TO_TEAM_ID).astype(object)
        tmp = tmp.dropna(subset=["team_id"])
        if not tmp.empty:
            bat_d = bat_d.merge(tmp[["player_id","team_id"]].drop_duplicates("player_id"),
                                on="player_id", how="left", suffixes=("","_fill"))
            use_fill = bat_d["team_id"].eq("") | bat_d["team_id"].isna()
            bat_d.loc[use_fill, "team_id"] = bat_d.loc[use_fill, "team_id_fill"]
            bat_d.drop(columns=[c for c in ["team_id_fill"] if c in bat_d.columns], inplace=True)

    # Fill game_id from team_id
    need_gid = bat_d["game_id"].eq("") | bat_d["game_id"].isna()
    if need_gid.any():
        bat_d = bat_d.merge(team_game, on="team_id", how="left", suffixes=("","_from_map"))
        need = bat_d["game_id"].eq("") | bat_d["game_id"].isna()
        bat_d.loc[need, "game_id"] = bat_d.loc[need, "game_id_from_map"]
        bat_d.drop(columns=[c for c in ["game_id_from_map"] if c in bat_d.columns], inplace=True)

    # Drop any rows that are still unresolved (off-slate or unmapped)
    unresolved = bat_d[(bat_d["team_id"].eq("") | bat_d["team_id"].isna()) |
                       (bat_d["game_id"].eq("") | bat_d["game_id"].isna())]
    if not unresolved.empty:
        unresolved[["player_id","team","team_id","game_id"]].to_csv(
            SUM_DIR / "batter_rows_dropped_unresolved_keys.csv", index=False
        )
        keep_mask = ~bat_d.index.isin(unresolved.index)
        dropped = int((~keep_mask).sum())
        bat_d = bat_d.loc[keep_mask].copy()

    return bat_d, dropped

# --- Main --------------------------------------------------------------------

def main():
    print("LOAD: daily & season inputs")
    bat_d = pd.read_csv(BATTERS_DAILY)
    bat_x = pd.read_csv(BATTERS_EXP)
    pit_d = pd.read_csv(PITCHERS_DAILY)
    bat_s = pd.read_csv(BATTERS_SEASON)
    pit_s = pd.read_csv(PITCHERS_SEASON)

    # Minimal presence (team/team_id/game_id may be empty; we heal below)
    require(bat_d, ["player_id","team","proj_pa_used"], str(BATTERS_DAILY))
    require(bat_x, ["player_id","game_id"] + [c for c in ADJ_COLS if c in bat_x.columns], str(BATTERS_EXP))
    require(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"], str(PITCHERS_DAILY))
    require(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(BATTERS_SEASON))
    require(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(PITCHERS_SEASON))

    # Heal & drop unresolved (instead of asserting)
    bat_d, dropped_n = heal_and_filter_keys(bat_d)
    if dropped_n > 0:
        print(f"[INFO] Dropped {dropped_n} batter rows with unresolved team_id/game_id "
              f"(see {SUM_DIR/'batter_rows_dropped_unresolved_keys.csv'})")

    # Coerce numerics now that keys exist
    to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
    to_num(bat_x, ["player_id","game_id"])
    to_num(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"])
    to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
    to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

    # Adjustment columns default to neutral
    for c in ADJ_COLS:
        if c not in bat_x.columns:
            bat_x[c] = 1.0
    bat_x[ADJ_COLS] = bat_x[ADJ_COLS].apply(pd.to_numeric, errors="coerce").fillna(1.0)

    # Confirm adjustments joinability
    keys_proj = set(zip(bat_d["player_id"], bat_d["game_id"]))
    keys_exp  = set(zip(bat_x["player_id"], bat_x["game_id"]))
    missing = keys_proj - keys_exp
    if missing:
        pd.DataFrame(list(missing), columns=["player_id","game_id"]).to_csv(
            SUM_DIR / "merge_mismatch_batters.csv", index=False
        )
        print("[WARN] some (player_id, game_id) not present in expanded; defaulting adj_woba_* = 1.0 for those rows.")
        miss_df = pd.DataFrame(list(missing), columns=["player_id","game_id"])
        for c in ADJ_COLS:
            miss_df[c] = 1.0
        bat_x = pd.concat([bat_x, miss_df], ignore_index=True)

    # Merge adjustments
    bat = bat_d.drop(columns=[c for c in ADJ_COLS if c in bat_d.columns], errors="ignore") \
               .merge(bat_x[["player_id","game_id"] + ADJ_COLS], on=["player_id","game_id"], how="left")
    for c in ADJ_COLS:
        bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(1.0).clip(lower=0)

    # Rates from season
    bat_rates = pd.DataFrame({
        "player_id": bat_s["player_id"],
        "p_k_b":  safe_rate(bat_s["strikeout"], bat_s["pa"]),
        "p_bb_b": safe_rate(bat_s["walk"],      bat_s["pa"]),
        "p_1b_b": safe_rate(bat_s["single"],    bat_s["pa"]),
        "p_2b_b": safe_rate(bat_s["double"],    bat_s["pa"]),
        "p_3b_b": safe_rate(bat_s["triple"],    bat_s["pa"]),
        "p_hr_b": safe_rate(bat_s["home_run"],  bat_s["pa"]),
    })
    pit_rates = pd.DataFrame({
        "player_id": pit_s["player_id"],
        "p_k_p":  safe_rate(pit_s["strikeout"], pit_s["pa"]),
        "p_bb_p": safe_rate(pit_s["walk"],      pit_s["pa"]),
        "p_1b_p": safe_rate(pit_s["single"],    pit_s["pa"]),
        "p_2b_p": safe_rate(pit_s["double"],    pit_s["pa"]),
        "p_3b_p": safe_rate(pit_s["triple"],    pit_s["pa"]),
        "p_hr_p": safe_rate(pit_s["home_run"],  pit_s["pa"]),
    })

    pit_d_enh = pit_d.merge(pit_rates, on="player_id", how="left")

    # Opponent starter map; if no starter (UNKNOWN), use league average for opponent
    rate_cols = ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
    opp_rates = (
        pit_d_enh.groupby(["game_id","opponent_team_id"], as_index=False)
                 .apply(lambda g: pd.Series(
                     {c: float(pd.to_numeric(g[c], errors="coerce").fillna(0)
                               .mul(pd.to_numeric(g["pa"], errors="coerce").fillna(0)).sum()
                               / max(float(pd.to_numeric(g["pa"], errors="coerce").fillna(0).sum()), 1.0))
                      for c in rate_cols}),
                        include_groups=False)
                 .rename(columns={"opponent_team_id":"team_id"})
    )

    # League averages for fallback
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa) if lg_pa > 0 else 0.0,
        "bb": float(bat_s["walk"].sum()      / lg_pa) if lg_pa > 0 else 0.0,
        "1b": float(bat_s["single"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "2b": float(bat_s["double"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "3b": float(bat_s["triple"].sum()    / lg_pa) if lg_pa > 0 else 0.0,
        "hr": float(bat_s["home_run"].sum()  / lg_pa) if lg_pa > 0 else 0.0,
    }
    lg_row = pd.DataFrame([{
        "p_k_p":lg["k"], "p_bb_p":lg["bb"], "p_1b_p":lg["1b"],
        "p_2b_p":lg["2b"], "p_3b_p":lg["3b"], "p_hr_p":lg["hr"]
    }])

    # Attach batter priors and opponent priors
    bat = bat.merge(bat_rates, on="player_id", how="left")
    bat = bat.merge(opp_rates, on=["game_id","team_id"], how="left", suffixes=("","_opp"))

    # Fill missing opponent rates with league averages
    for src, dst in zip(["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"],
                        ["p_k_opp","p_bb_opp","p_1b_opp","p_2b_opp","p_3b_opp","p_hr_opp"]):
        if dst not in bat.columns:
            bat[dst] = np.nan
        bat[dst] = bat[dst].fillna(lg_row.iloc[0][src])

    # LOG5 + ENV
    bat["p_k"]  = log5(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
    bat["p_bb"] = log5(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
    bat["p_1b"] = log5(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"])
    bat["p_2b"] = log5(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"])
    bat["p_3b"] = log5(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"])
    bat["p_hr"] = log5(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"])

    bat["p_1b"] *= bat["adj_woba_combined"]
    bat["p_2b"] *= bat["adj_woba_combined"]
    bat["p_3b"] *= bat["adj_woba_combined"]
    bat["p_hr"] *= bat["adj_woba_combined"]

    # Clamp and derive outs
    for c in ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]:
        bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    over = s > 1.0
    if over.any():
        bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]] = \
            bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].div(s[over], axis=0)
    bat["p_out"] = (1.0 - bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)).clip(0.0, 1.0)

    keep_cols = ["player_id","team_id","team","game_id","proj_pa_used",
                 "p_k","p_bb","p_1b","p_2b","p_3b","p_hr","p_out",
                 "adj_woba_weather","adj_woba_park","adj_woba_combined"]
    result = bat[keep_cols].copy()

    # Final sanity (no hard fail; just log anything odd)
    bad_tid = result["team_id"].isna() | result["team_id"].astype(str).eq("")
    bad_gid = result["game_id"].isna() | result["game_id"].astype(str).eq("")
    if bad_tid.any() or bad_gid.any():
        result.loc[bad_tid | bad_gid, ["player_id","team","team_id","game_id"]].to_csv(
            SUM_DIR / "bep_still_missing_after_heal.csv", index=False
        )
        # Filter them out to protect downstream aggregation
        result = result.loc[~(bad_tid | bad_gid)].copy()

    result.to_csv(OUT_FILE_PROJ, index=False)
    result.to_csv(OUT_FILE_FINAL, index=False)
    print(f"OK: wrote {OUT_FILE_PROJ} and {OUT_FILE_FINAL} rows={len(result)}")

if __name__ == "__main__":
    main()
