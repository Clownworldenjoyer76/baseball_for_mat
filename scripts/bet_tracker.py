# scripts/bet_tracker.py
import os
import csv
import math
import tempfile
import shutil
import pandas as pd

# ---------- File paths ----------
BATTER_PROPS_FILE  = 'data/_projections/batter_props_z_expanded.csv'
PITCHER_PROPS_FILE = 'data/_projections/pitcher_mega_z.csv'   # player_id,name,team,prop_type,line,value,z_score,mega_z,over_probability
FINAL_SCORES_FILE  = 'data/_projections/final_scores_projected.csv'
BATTER_STATS_FILE  = 'data/cleaned/batters_today.csv'

PLAYER_PROPS_OUT = 'data/bets/player_props_history.csv'
GAME_PROPS_OUT   = 'data/bets/game_props_history.csv'

# ---------- Utils ----------
def ensure_directory_exists(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def _as_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _prob_from_z(z):
    """Soft logistic to convert z-ish scores to a probability (fallback only)."""
    try:
        p = 1 / (1 + math.exp(-(float(z) or 0) * 0.9))
    except Exception:
        p = 0.65
    return max(0.50, min(0.98, p))

def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def _pick_col(df: pd.DataFrame, options: list[str]) -> str | None:
    cols = set(df.columns)
    for c in options:
        if c in cols:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in options:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def _schema_snapshot(label: str, df: pd.DataFrame, required: list[str], preferred: list[str]=None):
    preferred = preferred or []
    have = set(df.columns)
    found_req = [c for c in required if c in have]
    missing_req = [c for c in required if c not in have]
    found_pref = [c for c in preferred if c in have]
    print(f"🔎 {label} schema → required found={found_req} missing={missing_req} preferred found={found_pref}")

def _write_csv_atomic(path: str, df: pd.DataFrame, header: bool):
    ensure_directory_exists(path)
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="bettracker_", suffix=".csv")
    os.close(tmp_fd)
    try:
        df.to_csv(tmp_path, index=False, header=header, quoting=csv.QUOTE_ALL if header else csv.QUOTE_MINIMAL)
        shutil.move(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

def _round_cols(df: pd.DataFrame, cols_prec: dict):
    df = df.copy()
    for c, prec in cols_prec.items():
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').round(prec)
    return df

# ---------- Pitcher props: direct from pitcher_mega_z.csv ----------
def load_pitcher_props_simple(pitcher_df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Expect columns:
      player_id, name, team, prop_type (strikeouts|walks), line, value, z_score, mega_z, over_probability

    Output:
      name, team, prop_type (pitcher_strikeouts|walks_allowed), line, over_probability, projection, player_id
    """
    if pitcher_df_raw is None or pitcher_df_raw.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    df = _std_cols(pitcher_df_raw)
    if 'prop_type' not in df.columns:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    mask = df['prop_type'].astype(str).str.strip().str.lower().isin(['strikeouts', 'walks'])
    df = df.loc[mask].copy()
    if df.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    df['prop_type'] = (
        df['prop_type'].astype(str).str.strip().str.lower()
          .map({'strikeouts': 'pitcher_strikeouts', 'walks': 'walks_allowed'})
    )

    df['line'] = pd.to_numeric(df.get('line', pd.NA), errors='coerce')

    if 'over_probability' in df.columns:
        df['over_probability'] = pd.to_numeric(df['over_probability'], errors='coerce').clip(0.50, 0.98)
    else:
        z_src = None
        if 'mega_z' in df.columns:
            z_src = pd.to_numeric(df['mega_z'], errors='coerce')
        elif 'z_score' in df.columns:
            z_src = pd.to_numeric(df['z_score'], errors='coerce')
        df['over_probability'] = z_src.apply(_prob_from_z) if z_src is not None else 0.65

    df['projection'] = pd.to_numeric(df.get('value', pd.Series([None]*len(df))), errors='coerce')

    for c in ['name','team','player_id']:
        if c not in df.columns:
            df[c] = ''

    out = df[['name','team','prop_type','line','over_probability','projection','player_id']].dropna(
        subset=['name','team','prop_type','line','over_probability','projection']
    ).copy()

    out['over_probability'] = out['over_probability'].clip(0.50, 0.98)
    out = out.drop_duplicates(subset=['name','prop_type','line'], keep='first')
    return out

# ---------- Thresholds ----------
def _market_threshold(prop_type: str) -> float:
    p = (prop_type or "").lower().strip()
    if p == "home_runs": return 0.15
    if p == "hits": return 0.60
    if p == "total_bases": return 1.20
    if p == "pitcher_strikeouts": return 3.0
    if p == "walks_allowed": return 0.8
    return 0.2

def run_bet_tracker():
    # -------- Load inputs --------
    try:
        batter_df     = _std_cols(pd.read_csv(BATTER_PROPS_FILE))
        pitcher_df    = _std_cols(pd.read_csv(PITCHER_PROPS_FILE))
        games_df      = _std_cols(pd.read_csv(FINAL_SCORES_FILE))
        batter_stats  = _std_cols(pd.read_csv(BATTER_STATS_FILE))
    except FileNotFoundError as e:
        print(f"❌ Required input file not found - {e}")
        return
    except Exception as e:
        print(f"❌ Failed reading inputs - {e}")
        return

    # -------- Schema snapshots --------
    _schema_snapshot("batter_props", batter_df, ['name','team','prop_type','line','over_probability','projection'], ['player_id'])
    _schema_snapshot("pitcher_props", pitcher_df, ['name','team','prop_type','line'], ['over_probability','value','player_id'])
    _schema_snapshot("final_scores", games_df, ['home_team','away_team'], ['home_score','away_score','date','game_date'])
    _schema_snapshot("batter_stats", batter_stats, ['player_id'], ['ab','AB','hit','home_run'])

    # -------- Current date & integrity --------
    date_col = _pick_col(games_df, ['date','Date','game_date'])
    if not date_col:
        print("❌ Could not find a date column in final_scores_projected.csv.")
        return
    dates_counts = games_df[date_col].value_counts(dropna=False)
    if len(dates_counts.index) > 1:
        print(f"⚠️ Multiple dates detected in games file: {dates_counts.to_dict()}")
    current_date = dates_counts.index[0]
    print(f"📅 Using date: {current_date} (games={len(games_df)})")

    # -------- Batters (filters & thresholds) --------
    # Player ID consistency log
    if 'player_id' in batter_df.columns:
        missing_pid = batter_df['player_id'].isna().sum()
        print(f"🪪 batter_props missing player_id count: {missing_pid}")

    if 'player_id' in batter_stats.columns:
        batter_stats["player_id"] = batter_stats["player_id"].astype(str).str.strip()
    if 'player_id' in batter_df.columns:
        batter_df["player_id"] = batter_df["player_id"].astype(str).str.strip()

    ab_col   = _pick_col(batter_stats, ['ab','AB','at_bats'])
    hit_col  = _pick_col(batter_stats, ['hit','hits'])
    hr_col   = _pick_col(batter_stats, ['home_run','home_runs','HR'])
    if ab_col and hit_col and hr_col and 'player_id' in batter_stats.columns and 'player_id' in batter_df.columns:
        tmp = batter_df.merge(batter_stats[["player_id", ab_col, hit_col, hr_col]], on="player_id", how="left")
        tmp[ab_col]  = pd.to_numeric(tmp[ab_col], errors='coerce')
        tmp[hit_col] = pd.to_numeric(tmp[hit_col], errors='coerce')
        tmp[hr_col]  = pd.to_numeric(tmp[hr_col], errors='coerce')
        tmp["hr_rate"]  = (tmp[hr_col] / tmp[ab_col]).replace([float('inf')], 0).fillna(0)
        tmp["hit_rate"] = (tmp[hit_col] / tmp[ab_col]).replace([float('inf')], 0).fillna(0)
        # AB floor to reduce small-sample noise
        ab_floor = 20
        tmp = tmp[(tmp[ab_col] >= ab_floor) | tmp[ab_col].isna()].copy()

        before_b = len(tmp)
        def _b_ok(r):
            p = str(r.get("prop_type") or "").strip().lower()
            if p == "home_runs":
                return (r.get("hr_rate") or 0) >= 0.02
            if p in ("hits","total_bases"):
                return (r.get("hit_rate") or 0) >= 0.20
            return True
        tmp = tmp[tmp.apply(_b_ok, axis=1)].copy()
        print(f"🧽 batters filtered by AB floor & rates: {before_b} → {len(tmp)}")
        batter_df = tmp

    # enforce required columns
    for col in ["name","team","prop_type","line","over_probability","projection"]:
        if col not in batter_df.columns:
            batter_df[col] = pd.NA
    batter_df["source"] = "batter"

    # -------- Pitchers --------
    pitcher_std = load_pitcher_props_simple(pitcher_df)
    pitcher_std["source"] = "pitcher"

    # -------- Combine --------
    keep_cols = ['name','team','prop_type','line','over_probability','projection','source']
    combined = pd.concat(
        [
            batter_df[keep_cols],
            pitcher_std[keep_cols]
        ],
        ignore_index=True
    )

    # Per-market projection thresholds
    before_thresh = len(combined)
    combined = combined[
        combined.apply(lambda r: _as_float(r["projection"]) is not None and _as_float(r["projection"]) >= _market_threshold(r["prop_type"]), axis=1)
    ]
    print(f"🎚️ projection thresholds: {before_thresh} → {len(combined)}")

    # Probability clamps/gates
    combined['over_probability'] = pd.to_numeric(combined['over_probability'], errors='coerce').clip(0.50, 0.98)
    combined = combined.dropna(subset=["over_probability"])
    combined = combined.sort_values("over_probability", ascending=False)

    # De-dup granularity: (name, prop_type, line)
    before_dd = len(combined)
    combined = combined.drop_duplicates(subset=["name","prop_type","line"], keep="first")
    print(f"🧹 de-dup (name,prop_type,line): {before_dd} → {len(combined)}")

    # Edge (internal logging only)
    combined["edge"] = combined["over_probability"] - 0.50
    print("🏷️ Top edges:", combined.nlargest(5, "edge")[["name","prop_type","line","over_probability","projection","edge"]].to_dict("records"))

    # ---- Best Prop (top 3 overall) ----
    best_props_df = combined.head(3).copy()
    best_props_df["bet_type"] = "Best Prop"
    best_pairs = set(zip(best_props_df["name"], best_props_df["prop_type"], best_props_df["line"]))
    print("⭐ Best Props:", best_props_df[["name","prop_type","line","over_probability","projection"]].to_dict("records"))

    # ---- Per-game (up to 5 per matchup, max 3 per team) ----
    home_team_col = _pick_col(games_df, ['home_team'])
    away_team_col = _pick_col(games_df, ['away_team'])
    home_score_col = _pick_col(games_df, ['home_score','home_projection','home_proj','proj_home','home'])
    away_score_col = _pick_col(games_df, ['away_score','away_projection','away_proj','proj_away','away'])

    remaining = combined[~combined.apply(lambda r: (r["name"], r["prop_type"], r["line"]) in best_pairs, axis=1)]
    individual_props_df = pd.DataFrame()

    if home_team_col and away_team_col:
        games_unique = games_df.drop_duplicates(subset=[home_team_col, away_team_col])

        per_game = []
        for _, g in games_unique.iterrows():
            home, away = g[home_team_col], g[away_team_col]
            gp = remaining[(remaining["team"] == home) | (remaining["team"] == away)].copy()
            gp = gp.sort_values("over_probability", ascending=False).head(20)  # pre-cap
            # balance cap: max 3 per team
            gp_home = gp[gp["team"] == home].head(3)
            gp_away = gp[gp["team"] == away].head(3)
            gp_bal = pd.concat([gp_home, gp_away]).sort_values("over_probability", ascending=False).head(5)
            if not gp_bal.empty:
                t = gp_bal.copy()
                t["bet_type"] = "Individual Game"
                per_game.append(t)
        if per_game:
            individual_props_df = pd.concat(per_game, ignore_index=True)
        print(f"🎯 per-game selections created for {len(per_game)} matchups")
    else:
        print("ℹ️ Skipping per-game selections (missing team columns in games file).")

    # ---- Prepare outputs ----
    all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    all_props["date"] = current_date

    # normalize/round for output consistency
    all_props = _round_cols(all_props, {"over_probability": 4, "projection": 2, "line": 2})
    # sort for stable diffs
    all_props = all_props.sort_values(["date","bet_type","over_probability","name"], ascending=[True, False, False, True])

    player_props_to_save = all_props[['date','name','team','line','prop_type','bet_type']].copy()
    player_props_to_save["prop_correct"] = ""

    # ---- Idempotent write (append-safe by deduping file) ----
    ensure_directory_exists(PLAYER_PROPS_OUT)
    if os.path.exists(PLAYER_PROPS_OUT):
        existing = pd.read_csv(PLAYER_PROPS_OUT)
        combined_hist = pd.concat([existing, player_props_to_save], ignore_index=True)
        combined_hist = combined_hist.drop_duplicates(subset=["date","name","prop_type","line"], keep="last")
        _write_csv_atomic(PLAYER_PROPS_OUT, combined_hist, header=True)
    else:
        _write_csv_atomic(PLAYER_PROPS_OUT, player_props_to_save, header=True)

    # ---- Game props (only if we have score columns) ----
    if home_team_col and away_team_col:
        games_unique = games_df.drop_duplicates(subset=[home_team_col, away_team_col]).copy()
        if date_col not in games_unique.columns:
            games_unique[date_col] = current_date

        # favorite with numeric safety
        hs = pd.to_numeric(games_unique.get(home_score_col, pd.NA), errors='coerce')
        as_ = pd.to_numeric(games_unique.get(away_score_col, pd.NA), errors='coerce')
        na_rows = hs.isna() | as_.isna()
        if na_rows.any():
            print(f"⚠️ Favorite calc: {na_rows.sum()} rows missing numeric scores.")

        games_unique['favorite'] = games_unique.apply(
            lambda row: row[home_team_col]
            if _as_float(row.get(home_score_col)) is not None and _as_float(row.get(away_score_col)) is not None
               and float(row[home_score_col]) > float(row[away_score_col])
            else row[away_team_col],
            axis=1
        )

        game_props_to_save = games_unique[[date_col, home_team_col, away_team_col]].copy()
        game_props_to_save.columns = ['date','home_team','away_team']
        game_props_to_save['favorite'] = games_unique['favorite'].values
        game_props_to_save['favorite_correct'] = ''
        game_props_to_save['projected_real_run_total'] = (hs + as_).round(2)
        game_props_to_save['actual_real_run_total'] = ''
        game_props_to_save['run_total_diff'] = ''

        game_props_to_save = game_props_to_save.sort_values(["date","home_team","away_team"]).copy()

        ensure_directory_exists(GAME_PROPS_OUT)
        if os.path.exists(GAME_PROPS_OUT):
            existing_g = pd.read_csv(GAME_PROPS_OUT)
            combined_g = pd.concat([existing_g, game_props_to_save], ignore_index=True)
            combined_g = combined_g.drop_duplicates(subset=["date","home_team","away_team"], keep="last")
            _write_csv_atomic(GAME_PROPS_OUT, combined_g, header=True)
        else:
            _write_csv_atomic(GAME_PROPS_OUT, game_props_to_save, header=True)
    else:
        print("ℹ️ Skipping game props output (missing score/team columns).")

    # ---- Summary prints ----
    print(f"✅ Finished bet tracker for date: {current_date}")
    print(f"📝 Player props rows now: {sum(1 for _ in open(PLAYER_PROPS_OUT, 'r', encoding='utf-8')) - 1 if os.path.exists(PLAYER_PROPS_OUT) else 0}")
    if os.path.exists(GAME_PROPS_OUT):
        print(f"📗 Game props rows now: {sum(1 for _ in open(GAME_PROPS_OUT, 'r', encoding='utf-8')) - 1}")

if __name__ == '__main__':
    run_bet_tracker()
