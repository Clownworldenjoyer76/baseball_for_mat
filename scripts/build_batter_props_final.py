#!/usr/bin/env python3
import math
from pathlib import Path

import numpy as np
import pandas as pd

# ---- Paths ----
BATTER_IN  = Path("data/bets/prep/batter_props_bets.csv")
OUT_FILE   = Path("data/bets/prep/batter_props_final.csv")

PROJ_CANDIDATES = [
    Path("data/_projections/batter_props_z_expanded.csv"),
    Path("data/_projections/batter_props_projected.csv"),
]

# Optional AB/PA/G backfill if missing
AB_BACKFILL = Path("data/end_chain/final/bat_today_final.csv")

# ---- Constants ----
SEASON_GAMES_DEFAULT = 162.0
DEFAULT_PA_PER_GAME = 4.2
MAX_REASONABLE_LAMBDA = 3.0  # cap for obviously bad merges

# ---- Utils ----
def std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df

def read_first_existing(paths):
    for p in paths:
        if p.is_file():
            return std_cols(pd.read_csv(p))
    return None

def get_first_value(row, candidates, as_float=False, min_val=None):
    for c in candidates:
        if c in row and pd.notna(row[c]):
            v = row[c]
            if as_float:
                try:
                    v = float(v)
                except:
                    continue
                if min_val is not None and v < min_val:
                    continue
            return v
    return np.nan

def infer_games(row):
    games = get_first_value(row, ["games","gp","season_games","g"], as_float=True, min_val=1e-9)
    if pd.isna(games) or games <= 0:
        games = SEASON_GAMES_DEFAULT
    return float(games)

def poisson_over_prob(lmbda, line):
    if pd.isna(lmbda) or lmbda < 0 or pd.isna(line):
        return np.nan
    try:
        k = int(math.ceil(float(line)))
    except:
        return np.nan
    if k <= 0:
        return 1.0
    lam = float(lmbda)
    lam = min(lam, MAX_REASONABLE_LAMBDA)
    cdf = 0.0
    term = math.exp(-lam)
    cdf += term
    for i in range(1, k):
        term *= lam / i
        cdf += term
    return max(0.0, min(1.0, 1.0 - cdf))

def per_game_rate_from_season(row, season_cols, games=None):
    season_total = get_first_value(row, season_cols, as_float=True, min_val=0.0)
    if pd.isna(season_total):
        return np.nan
    g = games if games is not None else infer_games(row)
    if g <= 0:
        g = SEASON_GAMES_DEFAULT
    return float(season_total) / float(g)

# ---- Lambdas per prop ----
def hr_lambda_pg(row):
    v = get_first_value(row, ["proj_hr_pg","hr_pg","expected_hr_pg"], as_float=True, min_val=0.0)
    if not pd.isna(v): return v
    v = per_game_rate_from_season(row, ["proj_hr","season_proj_hr","avg_hr","hr_proj_season"])
    if not pd.isna(v): return v
    hr = get_first_value(row, ["hr"], as_float=True, min_val=0.0)
    pa = get_first_value(row, ["pa"], as_float=True, min_val=0.0)
    if not pd.isna(hr) and not pd.isna(pa) and pa > 0:
        pa_g = get_first_value(row, ["expected_pa","pa_per_game"], as_float=True, min_val=0.1)
        if pd.isna(pa_g): pa_g = DEFAULT_PA_PER_GAME
        return float(hr) / float(pa) * float(pa_g)
    return np.nan

def hits_lambda_pg(row):
    v = get_first_value(row, ["proj_hits_pg","hits_pg","expected_hits_pg","total_hits_projection_pg"], as_float=True, min_val=0.0)
    if not pd.isna(v): return v
    return per_game_rate_from_season(row, ["proj_hits","season_proj_hits","hits_proj_season","total_hits_projection","h","hits"])

def tb_lambda_pg(row):
    v = get_first_value(row, ["proj_tb_pg","tb_pg","expected_tb_pg","total_bases_projection_pg"], as_float=True, min_val=0.0)
    if not pd.isna(v): return v
    return per_game_rate_from_season(row, ["proj_tb","season_proj_tb","tb_proj_season","total_bases_projection","tb"])

def r_lambda_pg(row):
    v = get_first_value(row, ["proj_runs_pg","runs_pg","expected_runs_pg"], as_float=True, min_val=0.0)
    if not pd.isna(v): return v
    return per_game_rate_from_season(row, ["proj_runs","season_proj_runs","r","runs"])

def rbi_lambda_pg(row):
    v = get_first_value(row, ["proj_rbi_pg","rbi_pg","expected_rbi_pg"], as_float=True, min_val=0.0)
    if not pd.isna(v): return v
    return per_game_rate_from_season(row, ["proj_rbi","season_proj_rbi","rbi"])

def bb_lambda_pg(row):
    v = get_first_value(row, ["proj_bb_pg","bb_pg","walks_pg","expected_bb_pg"], as_float=True, min_val=0.0)
    if not pd.isna(v): return v
    return per_game_rate_from_season(row, ["proj_bb","season_proj_bb","bb","walks"])

def sb_lambda_pg(row):
    v = get_first_value(row, ["proj_sb_pg","sb_pg","expected_sb_pg","steals_pg"], as_float=True, min_val=0.0)
    if not pd.isna(v): return v
    return per_game_rate_from_season(row, ["proj_sb","season_proj_sb","sb","steals"])

PROP_TO_LAMBDA = {
    "hr": hr_lambda_pg,
    "home_run": hr_lambda_pg,
    "h": hits_lambda_pg,
    "hit": hits_lambda_pg,
    "hits": hits_lambda_pg,
    "tb": tb_lambda_pg,
    "total_bases": tb_lambda_pg,
    "r": r_lambda_pg,
    "run": r_lambda_pg,
    "runs": r_lambda_pg,
    "rbi": rbi_lambda_pg,
    "bb": bb_lambda_pg,
    "walk": bb_lambda_pg,
    "walks": bb_lambda_pg,
    "sb": sb_lambda_pg,
    "steal": sb_lambda_pg,
    "steals": sb_lambda_pg,
}

def normalize_prop_name(v: str) -> str:
    if not isinstance(v, str): return ""
    v = v.strip().lower()
    if v in ("home runs","home_run","homer","homers"): return "hr"
    if v in ("total bases","total_bases"): return "tb"
    if v in ("hit","hits"): return "h"
    if v in ("run","runs"): return "r"
    if v in ("walk","walks"): return "bb"
    if v in ("steal","steals"): return "sb"
    return v

def try_parse_float(series, default=np.nan):
    def conv(x):
        try: return float(x)
        except: return default
    return series.map(conv)

# ---- AB/PA/G backfill with ambiguity guard ----
def backfill_ab(bat: pd.DataFrame) -> pd.DataFrame:
    if "ab" in bat.columns:
        ab_obj = bat["ab"]
        # In case duplicate column names caused a DataFrame, force Series
        if isinstance(ab_obj, pd.DataFrame):
            ab_series = ab_obj.iloc[:, 0]
        else:
            ab_series = ab_obj
        try:
            if bool(pd.Series(ab_series).notna().any()):
                return bat
        except Exception:
            pass

    if not AB_BACKFILL.is_file():
        return bat

    src = std_cols(pd.read_csv(AB_BACKFILL))

    if "player_id" in bat.columns and "player_id" in src.columns:
        keys = ["player_id"]
    elif all(k in bat.columns for k in ("name","team")) and all(k in src.columns for k in ("name","team")):
        keys = ["name","team"]
    else:
        return bat

    cols = ["ab","pa","games","gp","season_games","expected_pa","pa_per_game"]
    cols = [c for c in cols if c in src.columns]

    if not cols:
        return bat

    merged = bat.merge(src[keys+cols].drop_duplicates(keys), on=keys, how="left", suffixes=("","_bf"))

    for c in ["ab","pa","games","gp","season_games","expected_pa","pa_per_game"]:
        if c in bat.columns:
            src_col = c if c in src.columns else f"{c}_bf"
            if src_col in merged.columns:
                merged[c] = merged[c].fillna(merged[src_col])
        else:
            src_col = c if c in src.columns else f"{c}_bf"
            if src_col in merged.columns:
                merged[c] = merged[src_col]

    return merged

# ---- Main ----
def main():
    if not BATTER_IN.is_file():
        print(f"❌ Missing {BATTER_IN}")
        return
    bets = std_cols(pd.read_csv(BATTER_IN))

    for need in ["prop","line","player_id","name","team"]:
        if need not in bets.columns:
            bets[need] = np.nan

    bets["prop"] = bets["prop"].astype(str).map(normalize_prop_name)
    bets["line"] = try_parse_float(bets["line"])

    proj = read_first_existing(PROJ_CANDIDATES)
    if proj is None:
        print("❌ No projection file found in:", ", ".join(map(str, PROJ_CANDIDATES)))
        return

    for need in ["player_id","name","team"]:
        if need not in proj.columns:
            proj[need] = np.nan

    if bets["player_id"].notna().any() and "player_id" in proj.columns:
        merged = bets.merge(proj, on="player_id", how="left", suffixes=("","_p"))
        nohit = merged.filter(like="_p").isna().all(axis=1)
        if nohit.any() and all(k in bets.columns for k in ("name","team")) and all(k in proj.columns for k in ("name","team")):
            fallback = bets[nohit].merge(proj, on=["name","team"], how="left", suffixes=("","_p2"))
            merged.loc[nohit, :] = fallback.reindex(merged.columns, axis=1).fillna(merged.loc[nohit, :])
    elif all(k in bets.columns for k in ("name","team")) and all(k in proj.columns for k in ("name","team")):
        merged = bets.merge(proj, on=["name","team"], how="left", suffixes=("","_p"))
    else:
        merged = bets.copy()

    bat = std_cols(merged)

    # Backfill AB/PA/G if missing
    bat = backfill_ab(bat)

    # Per-game lambda for each row/prop
    lambdas = []
    for _, row in bat.iterrows():
        prop = row.get("prop","")
        fn = PROP_TO_LAMBDA.get(prop)
        if fn is None:
            lambdas.append(np.nan)
            continue
        lmbda = fn(row)
        if pd.isna(lmbda):
            g = infer_games(row)
            if prop == "hr":
                lmbda = per_game_rate_from_season(row, ["hr","season_hr"], games=g)
            elif prop == "h":
                lmbda = per_game_rate_from_season(row, ["h","season_hits"], games=g)
            elif prop == "tb":
                lmbda = per_game_rate_from_season(row, ["tb","season_tb"], games=g)
            elif prop == "r":
                lmbda = per_game_rate_from_season(row, ["r","season_r"], games=g)
            elif prop == "rbi":
                lmbda = per_game_rate_from_season(row, ["rbi","season_rbi"], games=g)
            elif prop == "bb":
                lmbda = per_game_rate_from_season(row, ["bb","walks","season_bb","season_walks"], games=g)
            elif prop == "sb":
                lmbda = per_game_rate_from_season(row, ["sb","steals","season_sb","season_steals"], games=g)
        lambdas.append(lmbda)

    bat["lambda_pg"] = pd.to_numeric(pd.Series(lambdas), errors="coerce").clip(upper=MAX_REASONABLE_LAMBDA)

    bat["over_probability"] = [
        poisson_over_prob(lmbda, line) for lmbda, line in zip(bat["lambda_pg"], bat["line"])
    ]

    out_cols = [
        "player_id","name","team","prop","line",
        "value","batter_z","mega_z","over_probability",
        "opp_team","opp_pitcher_name","opp_pitcher_mega_z","opp_pitcher_z",
        "lambda_pg"
    ]
    out_cols = [c for c in out_cols if c in bat.columns]
    out = bat[out_cols].copy() if out_cols else bat.copy()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote: {OUT_FILE} (rows={len(out)})")

if __name__ == "__main__":
    main()
