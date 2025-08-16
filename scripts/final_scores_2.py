import pandas as pd
import numpy as np

# Load the datasets
try:
    batter_props = pd.read_csv('batter_props_final.csv')
    pitcher_props = pd.read_csv('pitcher_props_bets.csv')
    todays_games = pd.read_csv('todaysgames_normalized.csv')
    game_props_history = pd.read_csv('game_props_history.csv')
except FileNotFoundError as e:
    print(f"Error loading CSV file: {e}. Please make sure all input files are in the same directory.")
    exit()

# Handle duplicate game_ids in todays_games
todays_games.drop_duplicates(subset='game_id', keep='first', inplace=True)

# Create game_id for batter_props
game_id_map = {}
for _, row in todays_games.iterrows():
    key1 = (row['home_team_abbr'], row['away_team_abbr'])
    key2 = (row['away_team_abbr'], row['home_team_abbr'])
    game_id_map[key1] = row['game_id']
    game_id_map[key2] = row['game_id']

batter_props['game_id'] = batter_props.apply(
    lambda row: game_id_map.get((row['team'], row['opp_team'])),
    axis=1
)

# Aggregate pitcher props
pitcher_aggs = pitcher_props.pivot_table(
    index='game_id',
    columns='prop',
    values='value',
    aggfunc='sum'
).reset_index()
pitcher_aggs.columns = ['pitcher_' + str(col) for col in pitcher_aggs.columns]
pitcher_aggs.rename(columns={'pitcher_game_id': 'game_id'}, inplace=True)

# Aggregate batter props for projected scores
batter_runs = batter_props[batter_props['prop'] == 'Runs'].copy()

if not batter_runs.empty:
    game_to_teams = todays_games.set_index('game_id')[['home_team_abbr', 'away_team_abbr']].to_dict('index')

    def get_home_away(row):
        teams = game_to_teams.get(row['game_id'])
        if teams:
            if row['team'] == teams['home_team_abbr']:
                return 'home'
            elif row['team'] == teams['away_team_abbr']:
                return 'away'
        return 'unknown'

    batter_runs['home_away'] = batter_runs.apply(get_home_away, axis=1)

    batter_aggs = batter_runs.groupby(['game_id', 'home_away'])['value'].sum().unstack(fill_value=0)
    if 'home' in batter_aggs.columns:
        batter_aggs.rename(columns={'home': 'proj_home_score'}, inplace=True)
    else:
        batter_aggs['proj_home_score'] = 0
    if 'away' in batter_aggs.columns:
        batter_aggs.rename(columns={'away': 'proj_away_score'}, inplace=True)
    else:
        batter_aggs['proj_away_score'] = 0
    batter_aggs = batter_aggs.reset_index()
else:
    # If there are no 'Runs' props, create an empty DataFrame with the necessary columns
    batter_aggs = pd.DataFrame(columns=['game_id', 'proj_home_score', 'proj_away_score'])


# Merge data and finalize
merged_df = todays_games.merge(pitcher_aggs, on='game_id', how='left')
# Ensure batter_aggs is not empty before merging, or that it has the right columns.
if not batter_aggs.empty:
    merged_df = merged_df.merge(batter_aggs[['game_id', 'proj_home_score', 'proj_away_score']], on='game_id', how='left')
else:
    merged_df['proj_home_score'] = np.nan
    merged_df['proj_away_score'] = np.nan


output_df = pd.DataFrame(columns=game_props_history.columns)
for col in output_df.columns:
    if col in merged_df.columns:
        output_df[col] = merged_df[col]

# Fill projected run total, handling potential NaNs from the merge
output_df['projected_real_run_total'] = merged_df['proj_home_score'].fillna(0) + merged_df['proj_away_score'].fillna(0)


blank_cols = ['favorite_correct', 'home_score', 'away_score', 'actual_real_run_total']
for col in blank_cols:
    if col in output_df.columns:
        output_df[col] = np.nan

output_df.to_csv('game_props_history.csv', index=False)

print("Script finished. 'game_props_history.csv' has been updated.")
print("\nFinal DataFrame head:")
print(output_df.head())
