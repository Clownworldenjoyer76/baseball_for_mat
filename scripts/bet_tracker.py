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
GAME_PROPS_OUT = 'data/bets/game_props_history.csv'

def ensure_directory_exists(file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def _first_col(df, candidates):
    """Return the first existing column from candidates (case-insensitive)."""
    for c in candidates:
        if c in df.columns:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return ''

def _as_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _nearest_half(x: float) -> float:
    """Round to nearest 0.5 increment."""
    return round(x * 2) / 2.0

def _prob_from_z(z):
    """Gentle logistic -> probability; clamp to [0.55, 0.95] so it's usable."""
    try:
        p = 1 / (1 + math.exp(-(float(z) or 0) * 0.9))
    except Exception:
        p = 0.65
    return max(0.55, min(0.95, p))

def _explode_pitcher_props(pitcher_df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize pitcher props into rows:
      name, team, prop_type, line, over_probability, projection, player_id

    Supports:
      • Long/narrow files that already contain per-prop rows (name, team, prop_type, line, over_probability, [projection|mega_z])
      • Your current wide file with columns: player_id, name, team, strikeouts, walks, mega_z
      • Optional prob/line columns (will be used if present)
    """
    import numpy as np
    if pitcher_df_raw is None or pitcher_df_raw.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    df = pitcher_df_raw.copy()
    df.columns = [c.strip() for c in df.columns]

    # ---- CASE A: already long/narrow
    if {'name','team','prop_type','line','over_probability'}.issubset(df.columns):
        out = df[['name','team','prop_type','line','over_probability'] + [c for c in ['player_id','projection','mega_z'] if c in df.columns]].copy()
        # map prop_type labels to UI expectations
        out['prop_type'] = (
            out['prop_type'].astype(str).str.strip().str.lower()
            .map({'strikeouts': 'pitcher_strikeouts', 'walks': 'walks_allowed'})
            .fillna(out['prop_type'])
        )
        out['line'] = pd.to_numeric(out['line'], errors='coerce')
        out['over_probability'] = pd.to_numeric(out['over_probability'], errors='coerce')

        # projection: use provided, else |mega_z|, else 1.0
        if 'projection' in out.columns:
            out['projection'] = pd.to_numeric(out['projection'], errors='coerce')
        else:
            out['projection'] = pd.NA
        if 'mega_z' in out.columns:
            out['projection'] = out['projection'].fillna(pd.to_numeric(out['mega_z'], errors='coerce').abs())
        out['projection'] = out['projection'].fillna(1.0)

        out['player_id'] = out.get('player_id', '').astype(str).str.strip()
        out = out.dropna(subset=['name','team','prop_type','line','over_probability','projection'])
        return out[['name','team','prop_type','line','over_probability','projection','player_id']]

    # ---- CASE B: wide (your current pitcher_mega_z.csv)
    c_id   = _first_col(df, ['player_id','mlb_id','id'])
    c_name = _first_col(df, ['name','player_name','last_name, first_name','full_name'])
    c_team = _first_col(df, ['team','team_name','team_code'])
    c_k_proj = _first_col(df, ['strikeouts','ks','k_proj','k_projection'])
    c_bb_proj = _first_col(df, ['walks','bb','bb_proj','bb_projection'])
    c_megaz = _first_col(df, ['mega_z','z','megaZ'])

    # optional lines/probs if present
    c_k_line = _first_col(df, ['k_line','strikeouts_line','ks_line'])
    c_bb_line = _first_col(df, ['bb_line','walks_line','walks_allowed_line'])
    c_k_prob = _first_col(df, ['k_over_prob','k_over_probability','strikeouts_over_prob'])
    c_bb_prob = _first_col(df, ['bb_over_prob','walks_over_prob','walks_allowed_over_prob'])

    pieces = []

    # Strikeouts prop
    if c_k_proj and c_name and c_team:
        sub = df[[col for col in [c_name, c_team, c_k_proj, c_k_line, c_k_prob, c_id, c_megaz] if col]].copy()
        sub.rename(columns={c_name:'name', c_team:'team', c_k_proj:'projection'}, inplace=True)
        # line: given or nearest .5 from projection
        if c_k_line:
            sub['line'] = pd.to_numeric(sub[c_k_line], errors='coerce')
        else:
            sub['line'] = pd.to_numeric(sub['projection'], errors='coerce').apply(_nearest_half)
        # over_prob: given or derived from mega_z
        if c_k_prob:
            sub['over_probability'] = pd.to_numeric(sub[c_k_prob], errors='coerce')
        else:
            sub['over_probability'] = pd.to_numeric(sub.get(c_megaz, pd.Series([None]*len(sub))), errors='coerce').apply(_prob_from_z)
        sub['prop_type'] = 'pitcher_strikeouts'
        if c_id: sub['player_id'] = sub[c_id].astype(str).str.strip()
        pieces.append(sub[['name','team','prop_type','line','over_probability','projection'] + (['player_id'] if c_id else [])])

    # Walks allowed prop
    if c_bb_proj and c_name and c_team:
        sub = df[[col for col in [c_name, c_team, c_bb_proj, c_bb_line, c_bb_prob, c_id, c_megaz] if col]].copy()
        sub.rename(columns={c_name:'name', c_team:'team', c_bb_proj:'projection'}, inplace=True)
        if c_bb_line:
            sub['line'] = pd.to_numeric(sub[c_bb_line], errors='coerce')
        else:
            sub['line'] = pd.to_numeric(sub['projection'], errors='coerce').apply(_nearest_half)
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
    if 'player_id' not in out.columns: out['player_id'] = ''
    out = out.dropna(subset=['name','team','prop_type','line','over_probability','projection'])
    return out[['name','team','prop_type','line','over_probability','projection','player_id']]

def run_bet_tracker():
    try:
        batter_df = pd.read_csv(BATTER_PROPS_FILE)
        pitcher_df_raw = pd.read_csv(PITCHER_PROPS_FILE)
        games_df = pd.read_csv(FINAL_SCORES_FILE)
        batter_stats = pd.read_csv(BATTER_STATS_FILE)
        pitcher_stats = pd.read_csv(PITCHER_STATS_FILE)
    except FileNotFoundError as e:
        print(f"Error: Required input file not found - {e}")
        return

    # Discover current date from final_scores
    date_columns = ['date', 'Date', 'game_date']
    current_date_column = next((col for col in date_columns if col in games_df.columns), None)
    if not current_date_column:
        print("Error: Could not find a date column in final_scores_projected.csv.")
        return
    current_date = games_df[current_date_column].iloc[0]

    # --- Batters sanity filter ---
    batter_stats["player_id"] = batter_stats["player_id"].astype(str).str.strip()
    batter_df["player_id"] = batter_df.get("player_id", "").astype(str).str.strip()
    if {'player_id','ab','hit','home_run'}.issubset(batter_stats.columns):
        batter_df = batter_df.merge(batter_stats[["player_id", "ab", "hit", "home_run"]], on="player_id", how="left")
        batter_df["hr_rate"] = batter_df["home_run"] / batter_df["ab"]
        batter_df["hit_rate"] = batter_df["hit"] / batter_df["ab"]
    else:
        batter_df["hr_rate"] = 0.0
        batter_df["hit_rate"] = 0.0

    def is_batter_valid(row):
        if row.get("prop_type") == "home_runs":
            return (row.get("hr_rate") or 0) >= 0.02
        elif row.get("prop_type") in ["hits", "total_bases"]:
            return (row.get("hit_rate") or 0) >= 0.2
        return True

    batter_df = batter_df[batter_df.apply(is_batter_valid, axis=1)]
    for col in ["name","team","prop_type","line","over_probability","projection"]:
        if col not in batter_df.columns:
            batter_df[col] = pd.NA
    batter_df["source"] = "batter"

    # --- Pitchers (now generated from strikeouts / walks + mega_z) ---
    pitcher_exp = _explode_pitcher_props(pitcher_df_raw)

    # Optional k_rate screen if data available
    pitcher_stats["player_id"] = pitcher_stats["player_id"].astype(str).str.strip()
    if 'player_id' in pitcher_exp.columns and {'player_id','strikeouts','innings_pitched'}.issubset(pitcher_stats.columns):
        ps = pitcher_stats.copy()
        ps["k_rate"] = ps["strikeouts"] / ps["innings_pitched"]
        pitcher_exp = pitcher_exp.merge(ps[["player_id", "k_rate"]], on="player_id", how="left")
        pitcher_exp = pitcher_exp[pitcher_exp["k_rate"].fillna(1.0) >= 1.0]  # keep if >=1 K/IP

    pitcher_exp["source"] = "pitcher"

    # --- Combine, filter, rank ---
    combined = pd.concat(
        [
            batter_df[['name','team','prop_type','line','over_probability','projection','source']],
            pitcher_exp[['name','team','prop_type','line','over_probability','projection','source']]
        ],
        ignore_index=True
    )

    # Filters (same intent as original)
    combined = combined[combined["projection"].apply(lambda x: _as_float(x) is not None and _as_float(x) > 0.2)]
    combined = combined[combined["over_probability"].apply(lambda x: _as_float(x) is not None and _as_float(x) < 0.98)]
    combined['over_probability'] = combined['over_probability'].astype(float)
    combined = combined.sort_values("over_probability", ascending=False)

    # Allow multiple markets per player → de-dupe by (name, prop_type)
    combined = combined.drop_duplicates(subset=["name", "prop_type"], keep="first")

    # ---- Best Prop (top 3 overall) ----
    best_props_df = combined.head(3).copy()
    best_props_df["bet_type"] = "Best Prop"
    best_pairs = set(zip(best_props_df["name"], best_props_df["prop_type"]))

    # ---- Per-game (up to 5) ----
    remaining = combined[~combined.apply(lambda r: (r["name"], r["prop_type"]) in best_pairs, axis=1)]
    games_df = games_df.drop_duplicates(subset=["home_team", "away_team"])
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

    # ---- Save player props ----
    all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    all_props["date"] = current_date
    player_props_to_save = all_props[['date','name','team','line','prop_type','bet_type']].copy()
    player_props_to_save["prop_correct"] = ""

    ensure_directory_exists(PLAYER_PROPS_OUT)
    if not os.path.exists(PLAYER_PROPS_OUT):
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=True, quoting=csv.QUOTE_ALL)
    else:
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=False, mode='a', quoting=csv.QUOTE_ALL)

    # ---- Save game props (unchanged) ----
    game_props_to_save = games_df[['date', 'home_team', 'away_team']].copy()
    game_props_to_save['favorite'] = games_df.apply(
        lambda row: row['home_team'] if row['home_score'] > row['away_score'] else row['away_team'], axis=1
    )
    game_props_to_save['favorite_correct'] = ''
    game_props_to_save['projected_real_run_total'] = (games_df['home_score'] + games_df['away_score']).round(2)
    game_props_to_save['actual_real_run_total'] = ''
    game_props_to_save['run_total_diff'] = ''
    game_props_to_save = game_props_to_save[[
        'date', 'home_team', 'away_team',
        'favorite', 'favorite_correct',
        'projected_real_run_total', 'actual_real_run_total', 'run_total_diff'
    ]]

    ensure_directory_exists(GAME_PROPS_OUT)
    if not os.path.exists(GAME_PROPS_OUT):
        game_props_to_save.to_csv(GAME_PROPS_OUT, index=False, header=True)
    else:
        game_props_to_save.to_csv(GAME_PROPS_OUT, index=False, header=False, mode='a')

    print(f"✅ Bet tracker script finished successfully for date: {current_date}")

if __name__ == '__main__':
    run_bet_tracker()
