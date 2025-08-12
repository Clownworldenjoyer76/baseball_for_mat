# bet_prep_3.py (simplified to match bet_prep_2 merge style)
import pandas as pd
from datetime import datetime
import os

# -------- Config --------
sched_file = 'data/bets/mlb_sched.csv'
pitcher_props_file = 'data/_projections/pitcher_mega_z.csv'
output_dir = 'data/bets/prep'
output_file = os.path.join(output_dir, 'pitcher_props_bets.csv')

# -------- IO --------
os.makedirs(output_dir, exist_ok=True)

try:
    mlb_sched_df = pd.read_csv(sched_file)
    pitcher_df = pd.read_csv(pitcher_props_file)
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure input files are in the correct directory.")
    raise SystemExit(1)

# -------- Merge date and game_id from schedule (no aliasing; use team as-is) --------
mlb_sched_away = mlb_sched_df.rename(columns={'away_team': 'team'})
mlb_sched_home = mlb_sched_df.rename(columns={'home_team': 'team'})
mlb_sched_merged = pd.concat([mlb_sched_away, mlb_sched_home], ignore_index=True)

# Left join on 'team' exactly as provided
pitcher_df = pd.merge(
    pitcher_df,
    mlb_sched_merged[['team', 'date', 'game_id']],
    on='team',
    how='left'
)

# -------- Add, Rename, and Modify Columns --------
# Copy "name" -> "player"
pitcher_df['player'] = pitcher_df.get('name', '')

# Ensure 'prop' column exists (some inputs may use 'prop_type')
if 'prop_type' in pitcher_df.columns and 'prop' not in pitcher_df.columns:
    pitcher_df.rename(columns={'prop_type': 'prop'}, inplace=True)
elif 'prop' not in pitcher_df.columns:
    pitcher_df['prop'] = ''

# Static fields
pitcher_df['player_pos'] = 'pitcher'
pitcher_df['sport'] = 'Baseball'   # requested
pitcher_df['league'] = 'MLB'
pitcher_df['timestamp'] = datetime.now().isoformat(timespec='seconds')

# Ensure optional columns exist
for col in ['bet_type', 'prop_correct', 'book', 'price']:
    if col not in pitcher_df.columns:
        pitcher_df[col] = ''

# Cast game_id to string while preserving blanks (avoid writing "nan")
if 'game_id' in pitcher_df.columns:
    try:
        pitcher_df['game_id'] = pd.to_numeric(pitcher_df['game_id'], errors='coerce').astype('Int64').astype(str)
        pitcher_df.loc[pitcher_df['game_id'] == '<NA>', 'game_id'] = ''
    except Exception:
        pitcher_df['game_id'] = pitcher_df['game_id'].astype(str).replace({'nan': ''})

# Save
pitcher_df.to_csv(output_file, index=False)
print(f"Saved -> {output_file}")
