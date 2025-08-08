import pandas as pd
import os
import csv

# File paths
BATTER_PROPS_FILE = 'data/_projections/batter_props_z_expanded.csv'
PITCHER_PROPS_FILE = 'data/_projections/pitcher_mega_z.csv'
FINAL_SCORES_FILE = 'data/_projections/final_scores_projected.csv'

PLAYER_PROPS_OUT = 'data/bets/player_props_history.csv'
GAME_PROPS_OUT = 'data/bets/game_props_history.csv'

def ensure_directory_exists(file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def run_bet_tracker():
    try:
        batter_df = pd.read_csv(BATTER_PROPS_FILE)
        pitcher_df = pd.read_csv(PITCHER_PROPS_FILE)
        games_df = pd.read_csv(FINAL_SCORES_FILE)
    except FileNotFoundError as e:
        print(f"Error: Required input file not found - {e}")
        return

    # Identify date column
    date_columns = ['date', 'Date', 'game_date']
    current_date_column = next((col for col in date_columns if col in games_df.columns), None)
    if not current_date_column:
        print("Error: Could not find a date column in final_scores_projected.csv.")
        return

    current_date = games_df[current_date_column].iloc[0]

    # Combine props to identify top 3
    batter_df['source'] = 'batter'
    pitcher_df['source'] = 'pitcher'
    combined = pd.concat([batter_df, pitcher_df], ignore_index=True)

    top_props = combined.sort_values(by='over_probability', ascending=False).head(3)
    top_keys = set(zip(top_props['name'], top_props['team'], top_props['line'], top_props['prop_type']))

    def assign_bet_type(row):
        key = (row['name'], row['team'], row['line'], row['prop_type'])
        return 'Best Prop' if key in top_keys else 'Individual Game'

    batter_df['bet_type'] = batter_df.apply(assign_bet_type, axis=1)
    pitcher_df['bet_type'] = pitcher_df.apply(assign_bet_type, axis=1)

    best_props_df = pd.concat([
        batter_df[batter_df['bet_type'] == 'Best Prop'],
        pitcher_df[pitcher_df['bet_type'] == 'Best Prop']
    ], ignore_index=True)
    best_players = best_props_df['name'].unique()

    # Gather individual game props
    individual_props_list = []
    games = games_df[['home_team', 'away_team']].drop_duplicates()

    for _, game in games.iterrows():
        home, away = game['home_team'], game['away_team']

        game_batters = batter_df[
            ((batter_df['team'] == home) | (batter_df['team'] == away)) &
            (~batter_df['name'].isin(best_players))
        ].sort_values(by='over_probability', ascending=False).head(2)
        individual_props_list.append(game_batters)

        game_pitchers = pitcher_df[
            ((pitcher_df['team'] == home) | (pitcher_df['team'] == away)) &
            (~pitcher_df['name'].isin(best_players))
        ].sort_values(by='over_probability', ascending=False).head(1)
        individual_props_list.append(game_pitchers)

    if individual_props_list:
        individual_props_df = pd.concat(individual_props_list, ignore_index=True)
        all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)
    else:
        all_props = best_props_df.copy()

    # Add date and write player props
    all_props['date'] = current_date
    player_props_to_save = all_props[['date', 'name', 'team', 'line', 'prop_type', 'bet_type']].copy()
    player_props_to_save['prop_correct'] = ''

    ensure_directory_exists(PLAYER_PROPS_OUT)
    if not os.path.exists(PLAYER_PROPS_OUT):
        player_props_to_save.to_csv(
            PLAYER_PROPS_OUT,
            index=False,
            header=True,
            quoting=csv.QUOTE_ALL
        )
    else:
        player_props_to_save.to_csv(
            PLAYER_PROPS_OUT,
            index=False,
            header=False,
            mode='a',
            quoting=csv.QUOTE_ALL
        )

    # --- Game Props ---
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
