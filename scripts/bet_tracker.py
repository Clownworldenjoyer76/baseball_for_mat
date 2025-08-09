# scripts/bet_tracker.py
import math
import os
import csv
import pandas as pd

# File paths
BATTER_PROPS_FILE = 'data/_projections/batter_props_z_expanded.csv'
PITCHER_PROPS_FILE = 'data/_projections/pitcher_mega_z.csv'
FINAL_SCORES_FILE = 'data/_projections/final_scores_projected.csv'
BATTER_STATS_FILE = 'data/cleaned/batters_today.csv'
PITCHER_STATS_FILE = 'data/end_chain/cleaned/pitchers_xtra_normalized.csv'

PLAYER_PROPS_OUT = 'data/bets/player_props_history.csv'
GAME_PROPS_OUT   = 'data/bets/game_props_history.csv'

def ensure_directory_exists(file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def _first_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        lc = c.lower()
        if lc in lower:
            return lower[lc]
    return ''

def _as_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _nearest_half(x):
    try:
        return round(float(x) * 2) / 2.0
    except Exception:
        return None

def _prob_from_z(z):
    try:
        p = 1 / (1 + math.exp(-(float(z) or 0) * 0.9))
    except Exception:
        p = 0.65
    return max(0.55, min(0.95, p))

def _derive_k_projection_from_stats(stats: pd.DataFrame) -> pd.Series:
    if stats is None or stats.empty:
        return pd.Series(dtype=float)
    c_k   = _first_col(stats, ['proj_strikeouts','strikeouts_proj','season_strikeouts','strikeouts','k'])
    c_ip  = _first_col(stats, ['expected_ip','innings_pitched_per_start','ip_expected'])
    c_ip_hist = _first_col(stats, ['season_ip','innings_pitched','ip'])
    s_k  = pd.to_numeric(stats.get(c_k, pd.Series([None]*len(stats))), errors='coerce')
    # choose IP baseline
    if c_ip:
        s_ip = pd.to_numeric(stats[c_ip], errors='coerce')
    else:
        s_ip_hist = pd.to_numeric(stats.get(c_ip_hist, pd.Series([None]*len(stats))), errors='coerce')
        k_per9 = (s_k / s_ip_hist.replace({0: pd.NA})) * 9
        return (k_per9 * (6.0 / 9.0)).fillna(6.0)
    s_ip_hist2 = pd.to_numeric(stats.get(c_ip_hist, pd.Series([None]*len(stats))), errors='coerce')
    k_per9 = (s_k / s_ip_hist2.replace({0: pd.NA})) * 9
    return (k_per9 * (s_ip / 9.0)).fillna(6.0)

def _derive_bb_projection_from_stats(stats: pd.DataFrame) -> pd.Series:
    if stats is None or stats.empty:
        return pd.Series(dtype=float)
    c_bb  = _first_col(stats, ['proj_walks','walks_proj','season_walks','walks','bb'])
    c_ip  = _first_col(stats, ['expected_ip','innings_pitched_per_start','ip_expected'])
    c_ip_hist = _first_col(stats, ['season_ip','innings_pitched','ip'])
    s_bb = pd.to_numeric(stats.get(c_bb, pd.Series([None]*len(stats))), errors='coerce')
    if c_ip:
        s_ip = pd.to_numeric(stats[c_ip], errors='coerce')
    else:
        s_ip_hist = pd.to_numeric(stats.get(c_ip_hist, pd.Series([None]*len(stats))), errors='coerce')
        bb_per9 = (s_bb / s_ip_hist.replace({0: pd.NA})) * 9
        return (bb_per9 * (6.0 / 9.0)).fillna(2.0)
    s_ip_hist2 = pd.to_numeric(stats.get(c_ip_hist, pd.Series([None]*len(stats))), errors='coerce')
    bb_per9 = (s_bb / s_ip_hist2.replace({0: pd.NA})) * 9
    return (bb_per9 * (s_ip / 9.0)).fillna(2.0)

def _explode_pitcher_props(pitcher_df_raw: pd.DataFrame, pitcher_stats: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    if pitcher_df_raw is None or pitcher_df_raw.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    df = pitcher_df_raw.copy()
    df.columns = [c.strip() for c in df.columns]

    # Case 1: already long (per-prop rows present)
    if {'name','team','prop_type','line','over_probability'}.issubset(df.columns):
        out = df[['name','team','prop_type','line','over_probability'] + [c for c in ['player_id','projection','mega_z'] if c in df.columns]].copy()
        out['prop_type'] = (
            out['prop_type'].astype(str).str.strip().str.lower()
              .map({'strikeouts':'pitcher_strikeouts','walks':'walks_allowed'})
              .fillna(out['prop_type'])
        )
        out['line'] = pd.to_numeric(out['line'], errors='coerce')
        out['over_probability'] = pd.to_numeric(out['over_probability'], errors='coerce')
        if 'projection' in out.columns:
            out['projection'] = pd.to_numeric(out['projection'], errors='coerce')
        else:
            out['projection'] = pd.NA
        if 'mega_z' in out.columns:
            out['projection'] = out['projection'].fillna(pd.to_numeric(out['mega_z'], errors='coerce').abs())
        out['projection'] = out['projection'].fillna(1.0)
        out['player_id'] = out.get('player_id','').astype(str).str.strip()
        out = out.dropna(subset=['name','team','prop_type','line','over_probability','projection'])
        return out[['name','team','prop_type','line','over_probability','projection','player_id']]

    # Case 2: wide file
    c_id    = _first_col(df, ['player_id','mlb_id','id'])
    c_name  = _first_col(df, ['name','player_name','last_name, first_name','full_name'])
    c_team  = _first_col(df, ['team','team_name','team_code'])
    c_k_proj   = _first_col(df, ['strikeouts','ks','k_proj','k_projection'])  # may be missing
    c_bb_proj  = _first_col(df, ['walks','bb','bb_proj','bb_projection'])     # may be missing
    c_megaz = _first_col(df, ['mega_z','z','megaZ'])

    c_k_line  = _first_col(df, ['k_line','strikeouts_line','ks_line'])
    c_bb_line = _first_col(df, ['bb_line','walks_line','walks_allowed_line'])
    c_k_prob  = _first_col(df, ['k_over_prob','k_over_probability','strikeouts_over_prob'])
    c_bb_prob = _first_col(df, ['bb_over_prob','walks_over_prob','walks_allowed_over_prob'])

    pieces = []

    # Stats keyed by player_id (season)
    stats = pitcher_stats.copy() if pitcher_stats is not None else None
    if stats is not None:
        stats.columns = [c.strip() for c in stats.columns]
    if stats is not None:
        if 'player_id' in stats.columns:
            stats['player_id'] = stats['player_id'].astype(str).str.strip()
            stats = stats.set_index('player_id', drop=False)
        else:
            stats = None

    # Strikeouts
    if c_name and c_team and (c_k_proj or (stats is not None and c_id)):
        sub = df[[col for col in [c_name, c_team, c_id, c_k_proj, c_k_line, c_k_prob, c_megaz] if col]].copy()
        sub.rename(columns={c_name:'name', c_team:'team'}, inplace=True)
        if c_k_proj:
            sub['projection'] = pd.to_numeric(sub[c_k_proj], errors='coerce')
        else:
            sub['projection'] = pd.NA
            if stats is not None and c_id:
                ids = sub[c_id].astype(str).str.strip()
                avail = ids[ids.isin(stats.index)]
                if not avail.empty:
                    derived = _derive_k_projection_from_stats(stats.loc[avail])
                    # align by index positions of avail within sub
                    sub.loc[avail.index, 'projection'] = derived.values
            sub['projection'] = pd.to_numeric(sub['projection'], errors='coerce').fillna(6.0)
        if c_k_line:
            sub['line'] = pd.to_numeric(sub[c_k_line], errors='coerce')
        else:
            sub['line'] = sub['projection'].apply(_nearest_half)
        if c_k_prob:
            sub['over_probability'] = pd.to_numeric(sub[c_k_prob], errors='coerce')
        else:
            sub['over_probability'] = pd.to_numeric(sub.get(c_megaz, pd.Series([None]*len(sub))), errors='coerce').apply(_prob_from_z)
        sub['prop_type'] = 'pitcher_strikeouts'
        if c_id: sub['player_id'] = sub[c_id].astype(str).str.strip()
        pieces.append(sub[['name','team','prop_type','line','over_probability','projection'] + (['player_id'] if c_id else [])])

    # Walks allowed
    if c_name and c_team and (c_bb_proj or (stats is not None and c_id)):
        sub = df[[col for col in [c_name, c_team, c_id, c_bb_proj, c_bb_line, c_bb_prob, c_megaz] if col]].copy()
        sub.rename(columns={c_name:'name', c_team:'team'}, inplace=True)
        if c_bb_proj:
            sub['projection'] = pd.to_numeric(sub[c_bb_proj], errors='coerce')
        else:
            sub['projection'] = pd.NA
            if stats is not None and c_id:
                ids = sub[c_id].astype(str).str.strip()
                avail = ids[ids.isin(stats.index)]
                if not avail.empty:
                    derived_bb = _derive_bb_projection_from_stats(stats.loc[avail])
                    sub.loc[avail.index, 'projection'] = derived_bb.values
            sub['projection'] = pd.to_numeric(sub['projection'], errors='coerce').fillna(2.0)
        if c_bb_line:
            sub['line'] = pd.to_numeric(sub[c_bb_line], errors='coerce')
        else:
            sub['line'] = sub['projection'].apply(_nearest_half)
        if c_bb_prob:
            sub['over_probability'] = pd.to_numeric(sub[c_bb_prob], errors='coerce')
        else:
            sub['over_probability'] = pd.to_numeric(sub.get(c_megaz, pd.Series([None]*len(sub))), errors='coerce').apply(_prob_from_z)
        sub['prop_type'] = 'walks_allowed'
        if c_id: sub['player_id'] = sub[c_id].astype(str).str.strip()
        pieces.append(sub[['name','team','prop_type','line','over_probability','projection'] + (['player_id'] if c_id else [])])

    if not pieces:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    out = pd.concat(pieces, ignore_index=True)
    out['line'] = pd.to_numeric(out['line'], errors='coerce')
    out['over_probability'] = pd.to_numeric(out['over_probability'], errors='coerce')
    out['projection'] = pd.to_numeric(out['projection'], errors='coerce')
    if 'player_id' not in out.columns:
        out['player_id'] = ''
    out = out.dropna(subset=['name','team','prop_type','line','over_probability','projection'])
    return out[['name','team','prop_type','line','over_probability','projection','player_id']]

def run_bet_tracker():
    try:
        batter_df     = pd.read_csv(BATTER_PROPS_FILE)
        pitcher_df    = pd.read_csv(PITCHER_PROPS_FILE)
        games_df      = pd.read_csv(FINAL_SCORES_FILE)
        batter_stats  = pd.read_csv(BATTER_STATS_FILE)
        pitcher_stats = pd.read_csv(PITCHER_STATS_FILE)
    except FileNotFoundError as e:
        print(f"Error: Required input file not found - {e}")
        return

    # Date from final scores (works with 'date', 'Date', or 'game_date')
    date_columns = ['date', 'Date', 'game_date']
    current_date_column = next((col for col in date_columns if col in games_df.columns), None)
    if not current_date_column:
        print("Error: Could not find a date column in final_scores_projected.csv.")
        return
    current_date = games_df[current_date_column].iloc[0]

    # --- Batters sanity filter ---
    batter_stats["player_id"] = batter_stats["player_id"].astype(str).str.strip()
    batter_df["player_id"]    = batter_df.get("player_id", "").astype(str).str.strip()
    if {'player_id','ab','hit','home_run'}.issubset(batter_stats.columns):
        batter_df = batter_df.merge(batter_stats[["player_id","ab","hit","home_run"]], on="player_id", how="left")
        batter_df["hr_rate"]  = batter_df["home_run"] / batter_df["ab"]
        batter_df["hit_rate"] = batter_df["hit"] / batter_df["ab"]
    else:
        batter_df["hr_rate"]  = 0.0
        batter_df["hit_rate"] = 0.0

    def is_batter_valid(row):
        if row.get("prop_type") == "home_runs":
            return (row.get("hr_rate") or 0) >= 0.02
        elif row.get("prop_type") in ["hits","total_bases"]:
            return (row.get("hit_rate") or 0) >= 0.2
        return True

    batter_df = batter_df[batter_df.apply(is_batter_valid, axis=1)]
    for col in ["name","team","prop_type","line","over_probability","projection"]:
        if col not in batter_df.columns:
            batter_df[col] = pd.NA
    batter_df["source"] = "batter"

    # --- Pitchers (Ks & Walks from mega_z or derived from season stats) ---
    pitcher_exp = _explode_pitcher_props(pitcher_df, pitcher_stats)

    # Optional K/IP screen if available
    pitcher_stats["player_id"] = pitcher_stats["player_id"].astype(str).str.strip()
    if 'player_id' in pitcher_exp.columns and {'player_id','strikeouts','innings_pitched'}.issubset(pitcher_stats.columns):
        ps = pitcher_stats.copy()
        ps["k_rate"] = pd.to_numeric(ps["strikeouts"], errors='coerce') / pd.to_numeric(ps["innings_pitched"], errors='coerce').replace({0: pd.NA})
        pitcher_exp = pitcher_exp.merge(ps[["player_id","k_rate"]], on="player_id", how="left")
        pitcher_exp = pitcher_exp[pitcher_exp["k_rate"].fillna(1.0) >= 1.0]
        pitcher_exp.drop(columns=["k_rate"], inplace=True, errors="ignore")

    pitcher_exp["source"] = "pitcher"

    # --- Combine, filter, rank ---
    combined = pd.concat(
        [
            batter_df[['name','team','prop_type','line','over_probability','projection','source']],
            pitcher_exp[['name','team','prop_type','line','over_probability','projection','source']]
        ],
        ignore_index=True
    )

    combined = combined[combined["projection"].apply(lambda x: _as_float(x) is not None and _as_float(x) > 0.2)]
    combined = combined[combined["over_probability"].apply(lambda x: _as_float(x) is not None and _as_float(x) < 0.98)]
    combined['over_probability'] = combined['over_probability'].astype(float)
    combined = combined.sort_values("over_probability", ascending=False)

    # Allow multiple markets per player
    combined = combined.drop_duplicates(subset=["name","prop_type"], keep="first")

    # Best Prop (top 3)
    best_props_df = combined.head(3).copy()
    best_props_df["bet_type"] = "Best Prop"
    best_pairs = set(zip(best_props_df["name"], best_props_df["prop_type"]))

    # Per-game (up to 5 each)
    remaining = combined[~combined.apply(lambda r: (r["name"], r["prop_type"]) in best_pairs, axis=1)]
    games_df = games_df.drop_duplicates(subset=["home_team","away_team"])
    per_game = []
    for _, g in games_df.iterrows():
        home, away = g['home_team'], g['away_team']
        gp = remaining[(remaining["team"] == home) | (remaining["team"] == away)]
        gp = gp.sort_values("over_probability", ascending=False).head(5)
        if not gp.empty:
            t = gp.copy()
            t["bet_type"] = "Individual Game"
            per_game.append(t)
    individual_props_df = pd.concat(per_game, ignore_index=True) if per_game else pd.DataFrame()

    # Save player props
    all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    all_props["date"] = current_date
    player_props_to_save = all_props[['date','name','team','line','prop_type','bet_type']].copy()
    player_props_to_save["prop_correct"] = ""

    ensure_directory_exists(PLAYER_PROPS_OUT)
    if not os.path.exists(PLAYER_PROPS_OUT):
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=True, quoting=csv.QUOTE_ALL)
    else:
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=False, mode='a', quoting=csv.QUOTE_ALL)

    # Save game props (unchanged)
    game_props_to_save = games_df[['date', 'home_team', 'away_team']].copy()
    game_props_to_save['favorite'] = games_df.apply(
        lambda row: row['home_team'] if row['home_score'] > row['away_score'] else row['away_team'], axis=1
    )
    game_props_to_save['favorite_correct'] = ''
    game_props_to_save['projected_real_run_total'] = (games_df['home_score'] + games_df['away_score']).round(2)
    game_props_to_save['actual_real_run_total'] = ''
    game_props_to_save['run_total_diff'] = ''
    game_props_to_save = game_props_to_save[[
        'date','home_team','away_team',
        'favorite','favorite_correct',
        'projected_real_run_total','actual_real_run_total','run_total_diff'
    ]]

    ensure_directory_exists(GAME_PROPS_OUT)
    if not os.path.exists(GAME_PROPS_OUT):
        game_props_to_save.to_csv(GAME_PROPS_OUT, index=False, header=True)
    else:
        game_props_to_save.to_csv(GAME_PROPS_OUT, index=False, header=False, mode='a')

    print(f"✅ Bet tracker script finished successfully for date: {current_date}")

if __name__ == '__main__':
    run_bet_tracker()
