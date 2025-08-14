#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

SCHED_IN      = Path("data/bets/mlb_sched.csv")
BATTER_IN     = Path("data/bets/prep/batter_props_final.csv")   # has batter_z and opp pitcher detail
PITCHER_IN    = Path("data/bets/prep/pitcher_props_bets.csv")   # has pitcher z/mega_z per team
OUT_HISTORY   = Path("data/bets/game_props_history.csv")

# ----- tunables for projection (simple linear map to runs)
BASE_RUNS = 4.25     # neutral baseline per team
A_BAT     = 0.75     # weight for team batter strength
B_PIT     = 0.75     # penalty for opponent pitcher strength
MIN_RUNS  = 0.2      # floor on projected runs

def _norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _safe_num(s, default=np.nan):
    return pd.to_numeric(s, errors="coerce").fillna(default)

def load_schedule(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"❌ Missing schedule file: {path}")
    sch = pd.read_csv(path)
    sch.columns = [c.strip() for c in sch.columns]
    need = [c for c in ("home_team", "away_team") if c not in sch.columns]
    if need:
        raise SystemExit(f"❌ schedule missing columns: {need}")
    sch["home_team"] = _norm(sch["home_team"])
    sch["away_team"] = _norm(sch["away_team"])
    return sch

def load_batter_strength(path: Path) -> pd.DataFrame:
    """Aggregate batter strength by team (mean of batter_z across all rows)."""
    if not path.exists():
        print(f"⚠️ Missing batter file: {path} (using zeros).")
        return pd.DataFrame(columns=["team", "bat_strength"]).assign(bat_strength=0.0)
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    if "team" not in df.columns or "batter_z" not in df.columns:
        print("⚠️ batter file lacks 'team' or 'batter_z'; using zeros.")
        return pd.DataFrame(columns=["team", "bat_strength"]).assign(bat_strength=0.0)
    df["team"] = _norm(df["team"])
    df["batter_z"] = _safe_num(df["batter_z"], 0.0)
    agg = df.groupby("team", as_index=False)["batter_z"].mean().rename(columns={"batter_z": "bat_strength"})
    return agg

def load_pitcher_strength(path: Path) -> pd.DataFrame:
    """
    Aggregate pitcher strength by team.
    Prefer 'mega_z', else 'z_score'. Use mean as a simple team indicator.
    """
    if not path.exists():
        print(f"⚠️ Missing pitcher file: {path} (using zeros).")
        return pd.DataFrame(columns=["team", "pit_strength"]).assign(pit_strength=0.0)
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    if "team" not in df.columns:
        print("⚠️ pitcher file lacks 'team'; using zeros.")
        return pd.DataFrame(columns=["team", "pit_strength"]).assign(pit_strength=0.0)

    df["team"] = _norm(df["team"])
    if "mega_z" in df.columns:
        x = _safe_num(df["mega_z"], 0.0)
    elif "z_score" in df.columns:
        x = _safe_num(df["z_score"], 0.0)
    else:
        x = pd.Series(0.0, index=df.index)

    agg = df.assign(_z=x).groupby("team", as_index=False)["_z"].mean().rename(columns={"_z": "pit_strength"})
    return agg

def project_runs(bat_strength: float, opp_pit_strength: float) -> float:
    r = BASE_RUNS + A_BAT * bat_strength - B_PIT * opp_pit_strength
    return float(max(MIN_RUNS, r))

def main():
    # 1) Load base games and make a full copy
    sch = load_schedule(SCHED_IN)
    out = sch.copy()

    # 2) Add required columns (initialize)
    add_cols = {
        "home_score_proj": np.nan,
        "home_score_final": np.nan,
        "away_score_proj": np.nan,
        "away_score_final": np.nan,
        "projected_real_run_total": np.nan,
        "real_run_total": np.nan,
        "favorite_projected": "",
        "favorite_correct": "",
    }
    for col, val in add_cols.items():
        if col not in out.columns:
            out[col] = val

    # 3) Load strengths
    bat_team = load_batter_strength(BATTER_IN)     # team -> bat_strength
    pit_team = load_pitcher_strength(PITCHER_IN)   # team -> pit_strength

    # 4) Merge strengths onto schedule
    # home side gets its own bat_strength + away pitcher strength
    out = out.merge(bat_team.rename(columns={"team": "home_team", "bat_strength": "home_bat_strength"}),
                    on="home_team", how="left")
    out = out.merge(bat_team.rename(columns={"team": "away_team", "bat_strength": "away_bat_strength"}),
                    on="away_team", how="left")
    out = out.merge(pit_team.rename(columns={"team": "home_team", "pit_strength": "home_pit_strength"}),
                    on="home_team", how="left")
    out = out.merge(pit_team.rename(columns={"team": "away_team", "pit_strength": "away_pit_strength"}),
                    on="away_team", how="left")

    # Fill any missing strengths with 0
    for c in ["home_bat_strength","away_bat_strength","home_pit_strength","away_pit_strength"]:
        if c in out.columns:
            out[c] = _safe_num(out[c], 0.0).fillna(0.0)
        else:
            out[c] = 0.0

    # 5) Compute projected runs
    out["home_score_proj"] = out.apply(
        lambda r: project_runs(r["home_bat_strength"], r["away_pit_strength"]), axis=1
    )
    out["away_score_proj"] = out.apply(
        lambda r: project_runs(r["away_bat_strength"], r["home_pit_strength"]), axis=1
    )
    out["projected_real_run_total"] = out["home_score_proj"] + out["away_score_proj"]

    # 6) Decide favorite by projected runs
    def pick_favorite(row) -> str:
        if pd.isna(row["home_score_proj"]) or pd.isna(row["away_score_proj"]):
            return ""
        if row["home_score_proj"] > row["away_score_proj"]:
            return row["home_team"]
        if row["away_score_proj"] > row["home_score_proj"]:
            return row["away_team"]
        return "PICK'EM"

    out["favorite_projected"] = out.apply(pick_favorite, axis=1)

    # 7) Prepare columns order (keep all schedule cols first)
    tail = [
        "home_bat_strength","away_bat_strength","home_pit_strength","away_pit_strength",
        "home_score_proj","away_score_proj","projected_real_run_total",
        "home_score_final","away_score_final","real_run_total",
        "favorite_projected","favorite_correct",
    ]
    # ensure presence
    tail = [c for c in tail if c in out.columns]
    ordered = [c for c in sch.columns if c in out.columns] + [c for c in tail if c not in sch.columns]
    out = out[ordered]

    # 8) Write/update history with de-duplication
    OUT_HISTORY.parent.mkdir(parents=True, exist_ok=True)

    if OUT_HISTORY.exists():
        hist = pd.read_csv(OUT_HISTORY)
        hist.columns = [c.strip() for c in hist.columns]

        # Identify a game key for dedupe
        if "game_id" in out.columns and "game_id" in hist.columns:
            key_cols = ["game_id"]
        else:
            # fallback on date + teams if game_id not present
            key_cols = [c for c in ["date","home_team","away_team"] if c in out.columns and c in hist.columns]
            if not key_cols:
                key_cols = None

        if key_cols:
            # keep newest from 'out' for same key
            # normalize join key types
            for c in key_cols:
                hist[c] = hist[c].astype(str)
                out[c]  = out[c].astype(str)
            merged = pd.concat([hist[hist.columns], out[hist.columns.intersection(out.columns)]], ignore_index=True)
            merged = merged.drop_duplicates(subset=key_cols, keep="last")
            merged.to_csv(OUT_HISTORY, index=False)
        else:
            # no common key -> append
            merged = pd.concat([hist, out], ignore_index=True)
            merged.to_csv(OUT_HISTORY, index=False)
    else:
        out.to_csv(OUT_HISTORY, index=False)

    print(f"✅ Wrote/updated: {OUT_HISTORY} (rows={len(pd.read_csv(OUT_HISTORY))})")

if __name__ == "__main__":
    main()
