# scripts/bet_tracker.py
import os
import csv
import math
import pandas as pd

# ---------- File paths ----------
BATTER_PROPS_FILE = 'data/_projections/batter_props_z_expanded.csv'
PITCHER_PROPS_FILE = 'data/_projections/pitcher_mega_z.csv'  # has: player_id,name,team,prop_type,line,value,z_score,mega_z,over_probability
FINAL_SCORES_FILE = 'data/_projections/final_scores_projected.csv'
BATTER_STATS_FILE = 'data/cleaned/batters_today.csv'

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
    return max(0.55, min(0.98, p))


# ---------- Pitcher props: direct from pitcher_mega_z.csv ----------
def load_pitcher_props_simple(pitcher_df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Expect columns:
      player_id, name, team, prop_type (strikeouts|walks), line, value, z_score, mega_z, over_probability

    Output columns (standardized to match batter props):
      name, team, prop_type (pitcher_strikeouts|walks_allowed), line, over_probability, projection, player_id
    """
    if pitcher_df_raw is None or pitcher_df_raw.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    df = pitcher_df_raw.copy()
    df.columns = [c.strip() for c in df.columns]

    # Keep only the two pitcher markets we care about
    mask = df['prop_type'].astype(str).str.strip().str.lower().isin(['strikeouts', 'walks'])
    df = df.loc[mask].copy()
    if df.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    # Normalize types for the UI
    df['prop_type'] = (
        df['prop_type'].astype(str).str.strip().str.lower()
          .map({'strikeouts': 'pitcher_strikeouts', 'walks': 'walks_allowed'})
    )

    # Numeric fields
    df['line'] = pd.to_numeric(df['line'], errors='coerce')
    # Use given probability if present; otherwise derive from mega_z/z_score
    if 'over_probability' in df.columns:
        df['over_probability'] = pd.to_numeric(df['over_probability'], errors='coerce')
    else:
        # derive
        z_src = None
        if 'mega_z' in df.columns:
            z_src = pd.to_numeric(df['mega_z'], errors='coerce')
        elif 'z_score' in df.columns:
            z_src = pd.to_numeric(df['z_score'], errors='coerce')
        df['over_probability'] = z_src.apply(_prob_from_z) if z_src is not None else 0.65

    # Projection comes from 'value'
    df['projection'] = pd.to_numeric(df['value'], errors='coerce')

    # Keep essentials
    if 'player_id' not in df.columns:
        df['player_id'] = ''

    out = df[['name','team','prop_type','line','over_probability','projection','player_id']].dropna(
        subset=['name','team','prop_type','line','over_probability','projection']
    )

    # One row per (player, market)
    out = out.drop_duplicates(subset=['name','prop_type'], keep='first')
    return out


def run_bet_tracker():
    # -------- Load inputs --------
    try:
        batter_df     = pd.read_csv(BATTER_PROPS_FILE)
        pitcher_df    = pd.read_csv(PITCHER_PROPS_FILE)
        games_df      = pd.read_csv(FINAL_SCORES_FILE)
        batter_stats  = pd.read_csv(BATTER_STATS_FILE)
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

    # -------- Batters (light sanity filter; keep existing schema) --------
    batter_stats["player_id"] = batter_stats["player_id"].astype(str).str.strip()
    batter_df["player_id"]    = batter_df.get("player_id", "").astype(str).str.strip()

    if {'player_id','ab','hit','home_run'}.issubset(batter_stats.columns):
        tmp = batter_df.merge(batter_stats[["player_id","ab","hit","home_run"]], on="player_id", how="left")
        tmp["hr_rate"]  = tmp["home_run"] / tmp["ab"]
        tmp["hit_rate"] = tmp["hit"]      / tmp["ab"]
        def _b_ok(r):
            if r.get("prop_type") == "home_runs":
                return (r.get("hr_rate") or 0) >= 0.02
            if r.get("prop_type") in ["hits","total_bases"]:
                return (r.get("hit_rate") or 0) >= 0.2
            return True
        batter_df = tmp[tmp.apply(_b_ok, axis=1)].copy()

    # enforce required columns (some batter sources might miss)
    for col in ["name","team","prop_type","line","over_probability","projection"]:
        if col not in batter_df.columns:
            batter_df[col] = pd.NA
    batter_df["source"] = "batter"

    # -------- Pitchers (direct read from pitcher_mega_z.csv) --------
    pitcher_std = load_pitcher_props_simple(pitcher_df)
    pitcher_std["source"] = "pitcher"

    # -------- Combine, filter, rank --------
    combined = pd.concat(
        [
            batter_df[['name','team','prop_type','line','over_probability','projection','source']],
            pitcher_std[['name','team','prop_type','line','over_probability','projection','source']]
        ],
        ignore_index=True
    )

    # Quality gates (same intent as before)
    combined = combined[combined["projection"].apply(lambda x: _as_float(x) is not None and _as_float(x) > 0.2)]
    combined = combined[combined["over_probability"].apply(lambda x: _as_float(x) is not None and _as_float(x) < 0.98)]

    combined['over_probability'] = combined['over_probability'].astype(float)
    combined = combined.sort_values("over_probability", ascending=False)

    # Allow multiple markets per player → de-dupe by (name, prop_type)
    combined = combined.drop_duplicates(subset=["name","prop_type"], keep="first")

    # ---- Best Prop (top 3 overall) ----
    best_props_df = combined.head(3).copy()
    best_props_df["bet_type"] = "Best Prop"
    best_pairs = set(zip(best_props_df["name"], best_props_df["prop_type"]))

    # ---- Per-game (up to 5 per matchup) ----
    remaining = combined[~combined.apply(lambda r: (r["name"], r["prop_type"]) in best_pairs, axis=1)]
    games_unique = games_df.drop_duplicates(subset=["home_team", "away_team"])
    per_game = []
    for _, g in games_unique.iterrows():
        home, away = g['home_team'], g['away_team']
        gp = remaining[(remaining["team"] == home) | (remaining["team"] == away)]
        gp = gp.sort_values("over_probability", ascending=False).head(5)
        if not gp.empty:
            t = gp.copy()
            t["bet_type"] = "Individual Game"
            per_game.append(t)
    individual_props_df = pd.concat(per_game, ignore_index=True) if per_game else pd.DataFrame()

    # ---- Save player props history ----
    all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    all_props["date"] = current_date

    player_props_to_save = all_props[['date','name','team','line','prop_type','bet_type']].copy()
    player_props_to_save["prop_correct"] = ""

    ensure_directory_exists(PLAYER_PROPS_OUT)
    if not os.path.exists(PLAYER_PROPS_OUT):
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=True, quoting=csv.QUOTE_ALL)
    else:
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=False, mode='a', quoting=csv.QUOTE_ALL)

    # ---- Save game props (unchanged logic) ----
    # we assume games_df has home_score/away_score already (projected)
    game_props_to_save = games_unique[['date', 'home_team', 'away_team']].copy()
    game_props_to_save['favorite'] = games_unique.apply(
        lambda row: row['home_team'] if row['home_score'] > row['away_score'] else row['away_team'], axis=1
    )
    game_props_to_save['favorite_correct'] = ''
    game_props_to_save['projected_real_run_total'] = (games_unique['home_score'] + games_unique['away_score']).round(2)
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
