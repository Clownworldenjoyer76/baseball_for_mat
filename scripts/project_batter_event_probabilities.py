#!/usr/bin/env python3
# scripts/project_batter_event_probabilities.py
#
# Builds per-batter event probabilities for today's games using:
#  - Season priors (batters.csv, pitchers.csv)
#  - Today's opponent starter(s) (pitcher_props_projected_final.csv)
#  - Environmental adjustments (batter_props_expanded_final.csv)
#  - Today's schedule (todaysgames_normalized.csv) to map team -> team_id (if missing)
#
# Robust to cases where batter_props_projected_final.csv is missing team_id/game_id:
#   • game_id is pulled from batter_props_expanded_final.csv (by player_id, if absent)
#   • team_id is resolved from data/raw/todaysgames_normalized.csv using team names
#
# Outputs diagnostics in summaries/07_final on any key-coverage issues.

import re
import numpy as np
import pandas as pd
from pathlib import Path

# Paths
DAILY_DIR = Path("data/_projections")
SEASON_DIR = Path("data/Data")
RAW_DIR = Path("data/raw")
SUM_DIR = Path("summaries/07_final")
OUT_DIR = Path("data/end_chain/final")

SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_DAILY   = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP     = DAILY_DIR / "batter_props_expanded_final.csv"
PITCHERS_DAILY  = DAILY_DIR / "pitcher_props_projected_final.csv"
BATTERS_SEASON  = SEASON_DIR / "batters.csv"
PITCHERS_SEASON = SEASON_DIR / "pitchers.csv"
TGN_FILE        = RAW_DIR   / "todaysgames_normalized.csv"

OUT_EVENTS_FILE = OUT_DIR / "batter_event_projections.csv"   # (optional artifact)
# This script's primary role is to feed downstream steps; adjust as you wish.

ADJ_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

# -------------------------- helpers --------------------------

def write_text(path: Path, text: str):
    path.write_text(text, encoding="utf-8")

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

def weighted_mean(g, cols, wcol):
    w = pd.to_numeric(g[wcol], errors="coerce").fillna(0.0)
    den = float(w.sum())
    out = {}
    for c in cols:
        x = pd.to_numeric(g[c], errors="coerce").fillna(0.0)
        out[c] = float((x * w).sum() / den) if den > 0 else float(x.mean())
    return pd.Series(out)

def log5(b, p, lg):
    if lg <= 0:
        raise RuntimeError("League rate <= 0")
    b = pd.to_numeric(b, errors="coerce").fillna(0.0)
    p = pd.to_numeric(p, errors="coerce").fillna(0.0)
    return (b * p) / float(lg)

def norm_team_name(x: str) -> str:
    if not isinstance(x, str):
        return ""
    # collapse to a simple comparable token, e.g. "Houston Astros" -> "astros", "Blue Jays" -> "bluejays"
    z = re.sub(r"[^A-Za-z]", "", x).lower()
    # prefer last word if multiword (“houstonastros” -> “astros”), but keep “bluejays”, “redsox”, “whitesox”
    # detect compound nicknames that are well-known:
    compounds = {"bluejays", "redsox", "whitesox"}
    if z in compounds:
        return z
    # otherwise try to take trailing nickname if the original had spaces
    parts = re.split(r"\s+", x.strip())
    if len(parts) >= 2:
        last = re.sub(r"[^A-Za-z]", "", parts[-1]).lower()
        if last:
            return last
    return z

def build_team_name_to_id_map(tgn: pd.DataFrame):
    """
    Try hard to discover (team_name, team_id) for both sides from todaysgames_normalized.csv.
    Works with a variety of column namings.
    """
    # Candidate columns
    name_candidates_home = [c for c in tgn.columns if re.search(r"(home).*(team|name|abbr)$", c, re.I)]
    name_candidates_away = [c for c in tgn.columns if re.search(r"(away).*(team|name|abbr)$", c, re.I)]
    id_candidates_home   = [c for c in tgn.columns if re.search(r"(home).*(team_?id|id)$", c, re.I)]
    id_candidates_away   = [c for c in tgn.columns if re.search(r"(away).*(team_?id|id)$", c, re.I)]

    # Fallbacks commonly seen
    for c in ["home_team", "team_home", "home_name"]:
        if c in tgn.columns and c not in name_candidates_home:
            name_candidates_home.append(c)
    for c in ["away_team", "team_away", "away_name"]:
        if c in tgn.columns and c not in name_candidates_away:
            name_candidates_away.append(c)
    for c in ["home_team_id", "team_id_home", "home_id"]:
        if c in tgn.columns and c not in id_candidates_home:
            id_candidates_home.append(c)
    for c in ["away_team_id", "team_id_away", "away_id"]:
        if c in tgn.columns and c not in id_candidates_away:
            id_candidates_away.append(c)

    if not id_candidates_home or not id_candidates_away:
        raise RuntimeError("todaysgames_normalized.csv lacks recognizable team_id columns (home/away)")

    # Choose the first viable pair; prefer explicit *_team and *_team_id
    name_h = name_candidates_home[0] if name_candidates_home else None
    name_a = name_candidates_away[0] if name_candidates_away else None
    id_h   = id_candidates_home[0]
    id_a   = id_candidates_away[0]

    recs = []
    for _, r in tgn.iterrows():
        # Home
        team_name_h = r.get(name_h, None)
        team_id_h   = r.get(id_h, None)
        if pd.notna(team_name_h) and pd.notna(team_id_h):
            recs.append((norm_team_name(str(team_name_h)), int(pd.to_numeric(team_id_h, errors="coerce"))))
        # Away
        team_name_a = r.get(name_a, None)
        team_id_a   = r.get(id_a, None)
        if pd.notna(team_name_a) and pd.notna(team_id_a):
            recs.append((norm_team_name(str(team_name_a)), int(pd.to_numeric(team_id_a, errors="coerce"))))

    m = {}
    for k, v in recs:
        if k and (k not in m):
            m[k] = v
    return m

# -------------------------- main --------------------------

def main():
    print("LOAD: daily & season & schedule inputs")
    bat_d = pd.read_csv(BATTERS_DAILY)
    bat_x = pd.read_csv(BATTERS_EXP)
    pit_d = pd.read_csv(PITCHERS_DAILY)
    bat_s = pd.read_csv(BATTERS_SEASON)
    pit_s = pd.read_csv(PITCHERS_SEASON)

    # Schedule is optional unless we need to backfill team_id
    tgn = None
    if TGN_FILE.exists():
        try:
            tgn = pd.read_csv(TGN_FILE)
        except Exception:
            tgn = None

    # Minimal columns the daily batter file MUST have right now
    require(bat_d, ["player_id", "team", "proj_pa_used"], str(BATTERS_DAILY))

    # If game_id missing in bat_d, try to pull from expanded (by player_id)
    if "game_id" not in bat_d.columns:
        print("BACKFILL: adding game_id to batter daily from expanded file")
        require(bat_x, ["player_id", "game_id"] + ADJ_COLS, str(BATTERS_EXP))
        to_num(bat_x, ["player_id", "game_id"])
        bat_d = bat_d.merge(
            bat_x[["player_id", "game_id"]],
            on="player_id",
            how="left"
        )
        if bat_d["game_id"].isna().any():
            missing = bat_d.loc[bat_d["game_id"].isna(), ["player_id", "team"]]
            missing.to_csv(SUM_DIR / "batter_missing_game_id.csv", index=False)
            raise RuntimeError("Could not backfill game_id for some batters; see summaries/07_final/batter_missing_game_id.csv")

    # If team_id missing, try to map from schedule using 'team' name
    if "team_id" not in bat_d.columns:
        print("BACKFILL: resolving team_id for batters via todaysgames_normalized.csv")
        if tgn is None:
            raise RuntimeError("Need team_id, but cannot read todaysgames_normalized.csv")

        team_map = build_team_name_to_id_map(tgn)
        bat_d["team_id"] = bat_d["team"].map(lambda s: team_map.get(norm_team_name(str(s)), np.nan))
        if bat_d["team_id"].isna().any():
            bad = bat_d.loc[bat_d["team_id"].isna(), ["player_id", "team"]].drop_duplicates()
            bad.to_csv(SUM_DIR / "batter_unmapped_team_names.csv", index=False)
            raise RuntimeError("Failed to map some batter teams to team_id; see summaries/07_final/batter_unmapped_team_names.csv")

    # Now enforce full set for downstream logic
    require(bat_d, ["player_id","team_id","team","game_id","proj_pa_used"], str(BATTERS_DAILY))
    require(bat_x, ["player_id","game_id"] + ADJ_COLS, str(BATTERS_EXP))
    require(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"], str(PITCHERS_DAILY))
    require(bat_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(BATTERS_SEASON))
    require(pit_s, ["player_id","pa","strikeout","walk","single","double","triple","home_run"], str(PITCHERS_SEASON))

    to_num(bat_d, ["player_id","team_id","game_id","proj_pa_used"])
    to_num(bat_x, ["player_id","game_id"])
    to_num(pit_d, ["player_id","game_id","team_id","opponent_team_id","pa"])
    to_num(bat_s, ["pa","strikeout","walk","single","double","triple","home_run"])
    to_num(pit_s, ["pa","strikeout","walk","single","double","triple","home_run"])

    # Ensure every batter game exists among pitcher games (opponent starter coverage)
    bat_games = set(pd.unique(bat_d["game_id"]))
    pit_games = set(pd.unique(pit_d["game_id"]))
    missing_games = sorted(list(bat_games - pit_games))
    if missing_games:
        pd.DataFrame({"game_id": missing_games}).to_csv(
            SUM_DIR / "missing_opponent_starter_games.csv", index=False
        )
        raise RuntimeError(
            "One or more batter games lack an opponent starter in pitcher_props_projected_final.csv; "
            "see summaries/07_final/missing_opponent_starter_games.csv"
        )

    # Merge in environment adjustments
    print("MERGE: add adjustment cols from expanded file")
    bat = bat_d.drop(columns=[c for c in ADJ_COLS if c in bat_d.columns], errors="ignore").merge(
        bat_x[["player_id","game_id"] + ADJ_COLS],
        on=["player_id","game_id"],
        how="left"
    )
    for c in ADJ_COLS:
        if c not in bat.columns or bat[c].isna().any():
            bad = bat.loc[bat[c].isna()] if c in bat.columns else bat[["player_id","game_id"]]
            bad.to_csv(SUM_DIR / f"missing_{c}.csv", index=False)
            raise RuntimeError(f"{c} invalid after merge; see summaries/07_final/missing_{c}.csv")

    print("RATES: build season priors for batters and pitchers")
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

    # Attach pitcher season rates, then compute opponent starter aggregates by (game_id, opponent_team_id)
    pit_d_enh = pit_d.merge(pit_rates, on="player_id", how="left")
    rate_cols = ["p_k_p","p_bb_p","p_1b_p","p_2b_p","p_3b_p","p_hr_p"]
    opp_rates = (
        pit_d_enh
        .groupby(["game_id","opponent_team_id"], as_index=False)
        .apply(lambda g: weighted_mean(g, rate_cols, "pa"), include_groups=False)
        .rename(columns={
            "opponent_team_id":"team_id",
            "p_k_p":"p_k_opp","p_bb_p":"p_bb_opp","p_1b_p":"p_1b_opp",
            "p_2b_p":"p_2b_opp","p_3b_p":"p_3b_opp","p_hr_p":"p_hr_opp"
        })
    )

    print("JOIN: add batter season rates and opponent starter rates")
    bat = bat.merge(bat_rates, on="player_id", how="left")
    bat = bat.merge(opp_rates, on=["game_id","team_id"], how="left")

    need = ["p_k_b","p_bb_b","p_1b_b","p_2b_b","p_3b_b","p_hr_b",
            "p_k_opp","p_bb_opp","p_1b_opp","p_2b_opp","p_3b_opp","p_hr_opp"]
    if bat[need].isna().any().any():
        bat.loc[bat[need].isna().any(axis=1),
                ["player_id","game_id","team_id","team"]+need].to_csv(
            SUM_DIR / "missing_rates_after_join_batters.csv", index=False
        )
        raise RuntimeError("Null outcome rates after join; see summaries/07_final/missing_rates_after_join_batters.csv")

    print("LEAGUE: compute league-average event rates from season totals")
    lg_pa = float(pd.to_numeric(bat_s["pa"], errors="coerce").sum())
    lg = {
        "k":  float(bat_s["strikeout"].sum() / lg_pa),
        "bb": float(bat_s["walk"].sum()      / lg_pa),
        "1b": float(bat_s["single"].sum()    / lg_pa),
        "2b": float(bat_s["double"].sum()    / lg_pa),
        "3b": float(bat_s["triple"].sum()    / lg_pa),
        "hr": float(bat_s["home_run"].sum()  / lg_pa),
    }

    print("LOG5 + ENV: blend priors and apply park/weather adjustments")
    bat["p_k"]  = log5(bat["p_k_b"],  bat["p_k_opp"],  lg["k"])
    bat["p_bb"] = log5(bat["p_bb_b"], bat["p_bb_opp"], lg["bb"])
    bat["p_1b"] = log5(bat["p_1b_b"], bat["p_1b_opp"], lg["1b"])
    bat["p_2b"] = log5(bat["p_2b_b"], bat["p_2b_opp"], lg["2b"])
    bat["p_3b"] = log5(bat["p_3b_b"], bat["p_3b_opp"], lg["3b"])
    bat["p_hr"] = log5(bat["p_hr_b"], bat["p_hr_opp"], lg["hr"])

    for c in ["p_1b","p_2b","p_3b","p_hr"]:
        bat[c] = bat[c] * bat["adj_woba_combined"]

    print("CLAMP: probabilities and compute outs")
    for c in ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]:
        bat[c] = pd.to_numeric(bat[c], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    over = s > 1.0
    if over.any():
        bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]] = \
            bat.loc[over, ["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].div(s[over], axis=0)
        s = bat[["p_k","p_bb","p_1b","p_2b","p_3b","p_hr"]].sum(axis=1)
    bat["p_out"] = (1.0 - s).clip(0.0, 1.0)

    # Optional artifact for inspection (keep columns lean)
    keep_cols = [
        "player_id","team","team_id","game_id","proj_pa_used",
        "p_k","p_bb","p_1b","p_2b","p_3b","p_hr","p_out",
        "adj_woba_weather","adj_woba_park","adj_woba_combined"
    ]
    bat[keep_cols].to_csv(OUT_EVENTS_FILE, index=False)
    print(f"WROTE batter event projections: {len(bat)} rows -> {OUT_EVENTS_FILE}")

    write_text(SUM_DIR / "status_batter_events.txt", "OK project_batter_event_probabilities.py")
    write_text(SUM_DIR / "errors_batter_events.txt", "")
    write_text(SUM_DIR / "summary_batter_events.txt", f"rows={len(bat)} out={OUT_EVENTS_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        SUM_DIR.mkdir(parents=True, exist_ok=True)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_text(SUM_DIR / "status_batter_events.txt", "FAIL project_batter_event_probabilities.py")
        write_text(SUM_DIR / "errors_batter_events.txt", repr(e))
        raise
