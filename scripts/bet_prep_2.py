import pandas as pd

# Load the input files
mlb_sched_df = pd.read_csv('data/bets/mlb_sched.csv')
batter_props_df = pd.read_csv('data/_projections/batter_props_z_expanded.csv')

# Prepare mlb_sched_df for merging
# Create a copy for merging with 'away_team'
mlb_sched_away = mlb_sched_df.rename(columns={'away_team': 'team'})
# Create a copy for merging with 'home_team'
mlb_sched_home = mlb_sched_df.rename(columns={'home_team': 'team'})
# Concatenate the two dataframes to have all teams in a single column
mlb_sched_merged = pd.concat([mlb_sched_away, mlb_sched_home])

# Merge dataframes to add 'date' and 'game_id'
# Left merge to keep all rows from batter_props_df
batter_props_df = pd.merge(batter_props_df, mlb_sched_merged[['team', 'date', 'game_id']],
                         on='team', how='left')

# Add the new columns
batter_props_df['player_name'] = batter_props_df['name']
batter_props_df['player_pos'] = 'batter'
batter_props_df['prop_type'] = ''
batter_props_df['prop_line'] = ''
batter_props_df['bet_type'] = ''
batter_props_df['over_probability'] = ''
batter_props_df['prop_correct'] = ''

# Save the final dataframe to the output file
batter_props_df.to_csv('data/bets/prep/batter_props_bets.csv', index=False)

print("Script bet_prep_2.py completed successfully. Output file created at data/bets/prep/batter_props_bets.csv")
