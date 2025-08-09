# scripts/bet_tracker.py
import pandas as pd
import os
import csv

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
    """Return the first existing column (exact match) from candidates; else ''."""
    for c in candidates:
        if c in df.columns:
            return c
    # case-insensitive fallback
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return ''

def _as_float(s):
    try:
        return float(s)
    except Exception:
        return None

def _explode_pitcher_props(pitcher_df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize/expand pitcher model rows into standardized bet rows:
      name, team, prop_type, line, over_probability, projection, player_id
    Supports common variants for Ks and Walks (allowed).
    """
    if pitcher_df_raw is None or pitcher_df_raw.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    df = pitcher_df_raw.copy()
    # normalize typical id/name/team columns
    c_id   = _first_col(df, ['player_id','mlb_id','id'])
    c_name = _first_col(df, ['name','player_name','last_name, first_name','full_name'])
    c_team = _first_col(df, ['team','team_name','team_code'])

    # ---- Strikeouts (Ks) ----
    # lines/probabilities/projections often named variously; check a wide set
    c_k_line   = _first_col(df, ['k_line','strikeouts_line','ks_line','pitcher_strikeouts_line','line_k'])
    c_k_prob   = _first_col(df, ['k_over_prob','k_over_probability','strikeouts_over_prob',
                                 'over_probability_k','over_probability_strikeouts','prob_k_over'])
    c_k_proj   = _first_col(df, ['k_projection','strikeouts_projection','projection_k','k_proj'])

    k_rows = []
    if c_k_line:
        sub = df[[col for col in [c_name, c_team, c_k_line, c_k_prob, c_k_proj, c_id] if col]].copy()
        sub.rename(columns={
            c_name: 'name',
            c_team: 'team',
            c_k_line: 'line',
            c_k_prob: 'over_probability',
            c_k_proj: 'projection',
            c_id: 'player_id'
        }, inplace=True)
        sub['prop_type'] = 'pitcher_strikeouts'
        k_rows.append(sub)

    # ---- Walks Allowed (BB) ----
    c_bb_line = _first_col(df, ['bb_line','walks_line','walks_allowed_line','pitcher_walks_allowed_line','line_bb'])
    c_bb_prob = _first_col(df, ['bb_over_prob','walks_over_prob','walks_allowed_over_prob',
                                'over_probability_bb','over_probability_walks','prob_bb_over'])
    c_bb_proj = _first_col(df, ['bb_projection','walks_projection','walks_allowed_projection','projection_bb','bb_proj'])

    bb_rows = []
    if c_bb_line:
        sub = df[[col for col in [c_name, c_team, c_bb_line, c_bb_prob, c_bb_proj, c_id] if col]].copy()
        sub.rename(columns={
            c_name: 'name',
            c_team: 'team',
            c_bb_line: 'line',
            c_bb_prob: 'over_probability',
            c_bb_proj: 'projection',
            c_id: 'player_id'
        }, inplace=True)
        sub['prop_type'] = 'walks_allowed'
        bb_rows.append(sub)

    pieces = []
    if k_rows: pieces.append(pd.concat(k_rows, ignore_index=True))
    if bb_rows: pieces.append(pd.concat(bb_rows, ignore_index=True))
    if not pieces:
        # No recognized pitcher markets; return empty standardized frame
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    out = pd.concat(pieces, ignore_index=True)

    # Sanity: coerce numeric fields and drop rows without essential data
    out['line'] = out['line'].apply(_as_float)
    out['over_probability'] = out['over_probability'].apply(_as_float)
    out['projection'] = out['projection'].apply(_as_float)

    # Drop rows missing name/team/line/over_probability/projection
    out = out[ out['name'].astype(str).str.strip().ne('') ]
    out = out[ out['team'].astype(str).str.strip().ne('') ]
    out = out[ out['line'].apply(lambda x: x is not None) ]
    out = out[ out['over_probability'].apply(lambda x: x is not None) ]
    out = out[ out['projection'].apply(lambda x: x is not None) ]

    # Standardize dtypes
    out['player_id'] = out.get('player_id', '').astype(str).str.strip()

    # Ensure same columns in the final order we’ll use later
    out = out[['name','team','prop_type','line','over_probability','projection','player_id']]
    return out

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

    # --- Sanity Filter: Batters ---
    # Expect batter_df to already have: name, team, prop_type, line, over_probability, projection
    batter_stats["player_id"] = batter_stats["player_id"].astype(str).str.strip()
    batter_df["player_id"] = batter_df.get("player_id", "").astype(str).str.strip()
    # join AB/H/HR for rate screens
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

    # Keep only needed batter columns (and ensure presence)
    for col in ["name","team","prop_type","line","over_probability","projection"]:
        if col not in batter_df.columns:
            batter_df[col] = pd.NA
    batter_df["source"] = "batter"
    # -----------------------------

    # --- Sanity Filter: Pitchers ---
    # Explode pitcher model into standardized markets (Ks, Walks)
    pitcher_exp = _explode_pitcher_props(pitcher_df_raw)

    # Merge in k_rate sanity filter if available
    pitcher_stats["player_id"] = pitcher_stats["player_id"].astype(str).str.strip()
    pitcher_exp["player_id"] = pitcher_exp.get("player_id", "").astype(str).str.strip()
    if 'player_id' in pitcher_exp.columns and 'player_id' in pitcher_stats.columns and 'strikeouts' in pitcher_stats.columns and 'innings_pitched' in pitcher_stats.columns:
        pitcher_stats = pitcher_stats.copy()
        pitcher_stats["k_rate"] = pitcher_stats["strikeouts"] / pitcher_stats["innings_pitched"]
        pitcher_exp = pitcher_exp.merge(pitcher_stats[["player_id", "k_rate"]], on="player_id", how="left")
        pitcher_exp = pitcher_exp[pitcher_exp["k_rate"].fillna(0) >= 1.0]
    # If k_rate unavailable, leave as-is

    pitcher_exp["source"] = "pitcher"
    # -------------------------------

    # Combine batter + pitcher standardized rows
    combined = pd.concat([batter_df[['name','team','prop_type','line','over_probability','projection','source']],
                          pitcher_exp[['name','team','prop_type','line','over_probability','projection','source']]],
                         ignore_index=True)

    # Filter/quality gates consistent with your original logic
    combined = combined[combined["projection"].apply(lambda x: _as_float(x) is not None and _as_float(x) > 0.2)]
    combined = combined[combined["over_probability"].apply(lambda x: _as_float(x) is not None and _as_float(x) < 0.98)]

    # Sort by over_probability desc (highest first)
    combined['over_probability'] = combined['over_probability'].astype(float)
    combined = combined.sort_values("over_probability", ascending=False)

    # IMPORTANT: allow multiple markets per player -> de-dupe by (name, prop_type)
    combined = combined.drop_duplicates(subset=["name", "prop_type"], keep="first")

    # Step 1: Top 3 overall (Best Prop)
    best_props_df = combined.head(3).copy()
    best_props_df["bet_type"] = "Best Prop"
    best_players_props = set(zip(best_props_df["name"], best_props_df["prop_type"]))

    # Step 2: Up to 5 props per game for Individual Game (exclude already chosen Best Prop tuples)
    remaining = combined[~list(zip(combined["name"], combined["prop_type"])).__contains__]
    # The above trick isn’t directly vectorized; do explicit filter:
    remaining = combined[~combined.apply(lambda r: (r["name"], r["prop_type"]) in best_players_props, axis=1)]

    # Use unique games; your final_scores_projected has home/away/score
    games_df = games_df.drop_duplicates(subset=["home_team", "away_team"])
    individual_props_list = []
    for _, game in games_df.iterrows():
        home, away = game['home_team'], game['away_team']
        game_props = remaining[(remaining["team"] == home) | (remaining["team"] == away)]
        game_props = game_props.sort_values("over_probability", ascending=False).head(5)
        if not game_props.empty:
            tmp = game_props.copy()
            tmp["bet_type"] = "Individual Game"
            individual_props_list.append(tmp)

    individual_props_df = pd.concat(individual_props_list, ignore_index=True) if individual_props_list else pd.DataFrame()

    # Final list to save
    all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    all_props["date"] = current_date

    # Shape for history output (keep same columns you already use)
    player_props_to_save = all_props[['date', 'name', 'team', 'line', 'prop_type', 'bet_type']].copy()
    player_props_to_save["prop_correct"] = ""

    ensure_directory_exists(PLAYER_PROPS_OUT)
    if not os.path.exists(PLAYER_PROPS_OUT):
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=True, quoting=csv.QUOTE_ALL)
    else:
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=False, mode='a', quoting=csv.QUOTE_ALL)

    # ---------------- Game props (unchanged) ----------------
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
