#!/usr/bin/env python3
# scripts/final_scores_1.py  (robust team-key merge + compact 'WhiteSox'/'RedSox' handling; fixed DataFrame truthiness)
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Optional, Dict
import pandas as pd
import numpy as np
import re

# ---- Inputs / Output ----
SCHED_FILE   = Path("data/bets/mlb_sched.csv")
TODAY_FILE   = Path("data/raw/todaysgames_normalized.csv")
BATTER_FILE  = Path("data/bets/prep/batter_props_bets.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")
GAME_OUT     = Path("data/bets/game_props_history.csv")

# ---- Output schema (exact order) ----
OUT_COLS: List[str] = [
    "game_id","date","home_team","away_team","venue_name",
    "favorite","favorite_correct","projected_real_run_total","actual_real_run_total",
    "run_total_diff","home_score","away_score","game_time","pitcher_home","pitcher_away",
]

def _std(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = d.columns.str.strip().str.lower()
    return d

def _read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return None

def _first_present(d: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in d.columns:
            return c
    return None

def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols]

SPECIALS: Dict[str,str] = {
    "ARIZONA DIAMONDBACKS": "DIAMONDBACKS",
    "BOSTON RED SOX": "RED SOX",
    "CHICAGO WHITE SOX": "WHITE SOX",
    "ST LOUIS CARDINALS": "CARDINALS",
    "ST. LOUIS CARDINALS": "CARDINALS",
    "TORONTO BLUE JAYS": "BLUE JAYS",
    "D-BACKS": "DIAMONDBACKS",
    "DBACKS": "DIAMONDBACKS",
}

def _canon_team_name(s: str) -> str:
    if s is None:
        return ""
    t = re.sub(r"\s+", " ", str(s).strip().upper())
    t_compact = re.sub(r"[^A-Z]", "", t)
    if t_compact == "WHITESOX":
        return "WHITE SOX"
    if t_compact == "REDSOX":
        return "RED SOX"
    if t in SPECIALS:
        return SPECIALS[t]
    if t in {"WHITE SOX","RED SOX"}:
        return t
    parts = t.split(" ")
    if len(parts) >= 2:
        return " ".join(parts[-2:]) if parts[-2:] in (["WHITE","SOX"], ["RED","SOX"]) else parts[-1]
    return t

def _add_team_keys(df: pd.DataFrame, home_col: str, away_col: str, prefix: str) -> pd.DataFrame:
    df = df.copy()
    df[f"{prefix}_home_key"] = df[home_col].apply(_canon_team_name)
    df[f"{prefix}_away_key"] = df[away_col].apply(_canon_team_name)
    return df

def _team_key_cols(d: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    team  = _first_present(d, ["team","team_name","team_full","home_team","away_team"])
    opp   = _first_present(d, ["opponent","opp","vs_team","away_team","home_team"])
    gid   = _first_present(d, ["game_id","gamepk","game_pk","id"])
    return team, opp, gid

def _date_cols(d: pd.DataFrame) -> Optional[str]:
    return _first_present(d, ["date","game_date"])

def _team_run_col(d: pd.DataFrame) -> Optional[str]:
    return _first_present(d, ["projected_team_runs","team_proj_runs","proj_runs","expected_runs","xr","x_runs","runs_proj"])

def _game_total_col(d: pd.DataFrame) -> Optional[str]:
    return _first_present(d, ["projected_real_run_total","projected_game_total","proj_game_total","game_total_proj","expected_total_runs"])

def _derive_game_totals_from_team_rows(d: pd.DataFrame) -> Optional[pd.DataFrame]:
    df = d.copy()
    date_col = _date_cols(df)
    team_col, opp_col, gid_col = _team_key_cols(df)
    run_col = _team_run_col(df)
    if run_col is None:
        return None
    for c in [team_col, opp_col]:
        if c and c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    if "home_team" in df.columns and "away_team" in df.columns and team_col and opp_col:
        keys = [c for c in ["game_id","date","home_team","away_team"] if c in df.columns]
        df["_is_home"] = (df[team_col] == df["home_team"])
        home = (df[df["_is_home"]].groupby(keys)[run_col].sum().rename("home_proj"))
        away = (df[~df["_is_home"]].groupby(keys)[run_col].sum().rename("away_proj"))
        out = pd.concat([home, away], axis=1).reset_index()
        if "home_proj" in out.columns and "away_proj" in out.columns:
            out["projected_real_run_total"] = out["home_proj"] + out["away_proj"]
            return out
    return None

def _extract_game_totals(path: Path) -> Optional[pd.DataFrame]:
    raw = _read_csv(path)
    if raw is None:
        return None
    df = _std(raw)
    game_total = _game_total_col(df)
    if game_total:
        keys = [c for c in ["game_id","date","home_team","away_team"] if c in df.columns]
        out = df[keys + [game_total]].dropna(subset=[game_total]).drop_duplicates().copy()
        out = out.rename(columns={game_total: "projected_real_run_total"})
        for side in ["home_proj","away_proj"]:
            if side in df.columns and side not in out.columns:
                out = out.merge(df[keys+[side]].drop_duplicates(), on=keys, how="left")
        return out
    return _derive_game_totals_from_team_rows(df)

def main() -> None:
    # ---- Read schedule safely (fixes DataFrame truthiness bug) ----
    sched_df = _read_csv(SCHED_FILE)
    sched = _std(sched_df) if sched_df is not None else pd.DataFrame()
    if sched.empty:
        raise SystemExit(f"❌ Missing or unreadable {SCHED_FILE}")
    if "venue_name" not in sched.columns and "venue" in sched.columns:
        sched["venue_name"] = sched["venue"]
    needed = ["game_id","date","home_team","away_team","venue_name"]
    missing = [c for c in needed if c not in sched.columns]
    if missing:
        raise SystemExit(f"❌ {SCHED_FILE} missing columns: {missing}")
    base = sched[needed].drop_duplicates().copy()
    base = _add_team_keys(base, "home_team","away_team","sched")

    # todaysgames (for time + pitchers)
    today_raw = _read_csv(TODAY_FILE)
    if today_raw is not None:
        today = _std(today_raw)
        keep_today = [c for c in today.columns if c in {"date","home_team","away_team","game_time","pitcher_home","pitcher_away"}]
        today = today[keep_today].drop_duplicates()
        today = _add_team_keys(today, "home_team","away_team","today")

        # Stage 1: (date + keys)
        t1 = today.copy()
        if "date" in t1.columns:
            t1 = t1[t1["date"].notna() & t1["date"].astype(str).str.strip().ne("")]
        else:
            t1 = t1.iloc[0:0]
        if not t1.empty:
            base = base.merge(
                t1,
                left_on=["date","sched_home_key","sched_away_key"],
                right_on=["date","today_home_key","today_away_key"],
                how="left",
                suffixes=("","_t1"),
            )
        # Stage 2: keys-only backfill
        need = (
            base.get("pitcher_home", pd.Series([pd.NA]*len(base))).isna()
            | base.get("pitcher_home", pd.Series([""]*len(base))).astype(str).eq("")
            | base.get("pitcher_away", pd.Series([pd.NA]*len(base))).isna()
            | base.get("pitcher_away", pd.Series([""]*len(base))).astype(str).eq("")
        )
        if need.any():
            t2 = today.drop(columns=[c for c in ["date"] if c in today.columns]).drop_duplicates()
            base = base.merge(
                t2,
                left_on=["sched_home_key","sched_away_key"],
                right_on=["today_home_key","today_away_key"],
                how="left",
                suffixes=("","_t2"),
            )
            for col in ["game_time","pitcher_home","pitcher_away"]:
                if col in base.columns and f"{col}_t2" in base.columns:
                    base[col] = base[col].where(~need, base[f"{col}_t2"])
                    base.drop(columns=[f"{col}_t2"], inplace=True, errors="ignore")
        base.drop(columns=[c for c in base.columns if c.endswith("_home_key") or c.endswith("_away_key")], inplace=True, errors="ignore")
    else:
        base["game_time"] = pd.NA
        base["pitcher_home"] = pd.NA
        base["pitcher_away"] = pd.NA

    # Projections
    gb = _extract_game_totals(BATTER_FILE)
    gp = _extract_game_totals(PITCHER_FILE)

    out = base.copy()
    proj = gb if gp is None else gp if gb is None else None
    if proj is None and gb is not None and gp is not None:
        keys = [k for k in ["game_id","date","home_team","away_team"] if k in out.columns]
        proj = gb.merge(gp, on=[k for k in keys if k in gb.columns and k in gp.columns], how="outer", suffixes=("_b","_p"))
        proj["projected_real_run_total"] = proj[["projected_real_run_total_b","projected_real_run_total_p"]].mean(axis=1, skipna=True)
        for side in ["home","away"]:
            cols = [f"{side}_proj_b", f"{side}_proj_p"]
            present = [c for c in cols if c in proj.columns]
            if present:
                proj[f"{side}_proj"] = proj[present].mean(axis=1, skipna=True)
        keep = [k for k in ["game_id","date","home_team","away_team","home_proj","away_proj","projected_real_run_total"] if k in proj.columns]
        proj = proj[keep].drop_duplicates()

    if proj is not None:
        keys = [k for k in ["game_id","date","home_team","away_team"] if k in out.columns and k in proj.columns]
        out = out.merge(proj, on=keys, how="left")
        if "home_proj" in out.columns and "away_proj" in out.columns:
            out["favorite"] = np.where(out["home_proj"] > out["away_proj"], out["home_team"],
                                np.where(out["away_proj"] > out["home_proj"], out["away_team"], pd.NA))
        else:
            out["favorite"] = pd.NA
    else:
        out["projected_real_run_total"] = pd.NA
        out["favorite"] = pd.NA

    # Fixed columns
    out["favorite_correct"] = pd.NA
    out["actual_real_run_total"] = pd.NA
    out["run_total_diff"] = pd.NA
    out["home_score"] = pd.NA
    out["away_score"] = pd.NA

    out = _ensure_cols(out, OUT_COLS)
    sort_cols = [c for c in ["date","game_id"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    GAME_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(GAME_OUT, index=False)
    print(f"✅ Wrote {len(out):,} rows → {GAME_OUT}")

if __name__ == "__main__":
    main()
