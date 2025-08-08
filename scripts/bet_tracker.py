
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

def run_bet_tracker():
    try:
        batter_df = pd.read_csv(BATTER_PROPS_FILE)
        pitcher_df = pd.read_csv(PITCHER_PROPS_FILE)
        games_df = pd.read_csv(FINAL_SCORES_FILE)
        batter_stats = pd.read_csv(BATTER_STATS_FILE)
        pitcher_stats = pd.read_csv(PITCHER_STATS_FILE)
    except FileNotFoundError as e:
        print(f"Error: Required input file not found - {e}")
        return

    date_columns = ['date', 'Date', 'game_date']
    current_date_column = next((col for col in date_columns if col in games_df.columns), None)
    if not current_date_column:
        print("Error: Could not find a date column in final_scores_projected.csv.")
        return
    current_date = games_df[current_date_column].iloc[0]

    # --- Sanity Filter: Batters
    batter_stats["player_id"] = batter_stats["player_id"].astype(str).str.strip()
    batter_df["player_id"] = batter_df["player_id"].astype(str).str.strip()
    batter_df = batter_df.merge(batter_stats[["player_id", "ab", "hit", "home_run"]], on="player_id", how="left")
    batter_df["hr_rate"] = batter_df["home_run"] / batter_df["ab"]
    batter_df["hit_rate"] = batter_df["hit"] / batter_df["ab"]

    def is_batter_valid(row):
        if row["prop_type"] == "home_runs":
            return row["hr_rate"] >= 0.02
        elif row["prop_type"] in ["hits", "total_bases"]:
            return row["hit_rate"] >= 0.2
        return True

    batter_df = batter_df[batter_df.apply(is_batter_valid, axis=1)]

    # --- Sanity Filter: Pitchers
    pitcher_stats["player_id"] = pitcher_stats["player_id"].astype(str).str.strip()
    pitcher_df["player_id"] = pitcher_df["player_id"].astype(str).str.strip()
    pitcher_stats["k_rate"] = pitcher_stats["strikeouts"] / pitcher_stats["innings_pitched"]
    pitcher_df = pitcher_df.merge(pitcher_stats[["player_id", "k_rate"]], on="player_id", how="left")
    pitcher_df = pitcher_df[pitcher_df["k_rate"] >= 1.0]

    # Combine and de-dupe by player
    batter_df["source"] = "batter"
    pitcher_df["source"] = "pitcher"
    combined = pd.concat([batter_df, pitcher_df], ignore_index=True)
    combined = combined[combined["projection"] > 0.2]
    combined = combined[combined["over_probability"] < 0.98]
    combined = combined.sort_values("over_probability", ascending=False)
    combined = combined.drop_duplicates(subset=["name"], keep="first")

    # Step 1: Top 3 props overall (Best Prop)
    best_props_df = combined.head(3).copy()
    best_props_df["bet_type"] = "Best Prop"
    best_players = set(best_props_df["name"])

    # Step 2: Filter rest, assign up to 5 props per game
    remaining = combined[~combined["name"].isin(best_players)]
    games_df = games_df.drop_duplicates(subset=["home_team", "away_team"])
    individual_props_list = []

    for _, game in games_df.iterrows():
        home, away = game['home_team'], game['away_team']
        game_props = remaining[(remaining["team"] == home) | (remaining["team"] == away)]
        game_props = game_props.sort_values("over_probability", ascending=False).head(5)
        game_props["bet_type"] = "Individual Game"
        individual_props_list.append(game_props)

    individual_props_df = pd.concat(individual_props_list, ignore_index=True) if individual_props_list else pd.DataFrame()
    all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    all_props["date"] = current_date
    player_props_to_save = all_props[['date', 'name', 'team', 'line', 'prop_type', 'bet_type']].copy()
    player_props_to_save["prop_correct"] = ""

    ensure_directory_exists(PLAYER_PROPS_OUT)
    if not os.path.exists(PLAYER_PROPS_OUT):
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=True, quoting=csv.QUOTE_ALL)
    else:
        player_props_to_save.to_csv(PLAYER_PROPS_OUT, index=False, header=False, mode='a', quoting=csv.QUOTE_ALL)

    # Game props
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
