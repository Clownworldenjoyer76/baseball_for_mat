# bet_prep_3.py
import pandas as pd
from datetime import datetime
import os

# -------- Config --------
sched_file = 'data/bets/mlb_sched.csv'
pitcher_props_file = 'data/_projections/pitcher_mega_z.csv'
team_map_file = 'data/Data/team_name_master.csv'
todays_games_file = 'data/raw/todaysgames_normalized.csv'
output_dir = 'data/bets/prep'
output_file = os.path.join(output_dir, 'pitcher_props_bets.csv')

# -------- IO --------
os.makedirs(output_dir, exist_ok=True)

try:
    mlb_sched_df = pd.read_csv(sched_file)
    pitcher_df = pd.read_csv(pitcher_props_file)
    team_map_df = pd.read_csv(team_map_file)
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure input files are in the correct directory.")
    raise SystemExit(1)

# -------- STEP 1: Normalize team names using team_name_master (clean_team_name -> team_name) --------
team_map_df['clean_team_name'] = team_map_df['clean_team_name'].astype(str).str.strip().str.lower()
team_map_df['team_name'] = team_map_df['team_name'].astype(str).str.strip()

if 'team' not in pitcher_df.columns:
    pitcher_df['team'] = ''

pitcher_df['_team_norm'] = pitcher_df['team'].astype(str).str.strip().str.lower()

pitcher_df = pitcher_df.merge(
    team_map_df[['clean_team_name', 'team_name']].rename(
        columns={'clean_team_name': '_team_norm', 'team_name': '_team_mapped'}
    ),
    on='_team_norm',
    how='left'
)

pitcher_df['team'] = pitcher_df['_team_mapped'].fillna(pitcher_df['team'])
pitcher_df.drop(columns=['_team_norm', '_team_mapped'], errors='ignore', inplace=True)

# -------- Merge date and game_id from schedule (using normalized team) --------
mlb_sched_away = mlb_sched_df.rename(columns={'away_team': 'team'})
mlb_sched_home = mlb_sched_df.rename(columns={'home_team': 'team'})
mlb_sched_merged = pd.concat([mlb_sched_away, mlb_sched_home], ignore_index=True)

pitcher_df = pd.merge(
    pitcher_df,
    mlb_sched_merged[['team', 'date', 'game_id']],
    on='team',
    how='left'
)

# -------- Add, Rename, and Modify Columns --------
pitcher_df['player'] = pitcher_df.get('name', '')

if 'prop_type' in pitcher_df.columns and 'prop' not in pitcher_df.columns:
    pitcher_df.rename(columns={'prop_type': 'prop'}, inplace=True)
elif 'prop' not in pitcher_df.columns:
    pitcher_df['prop'] = ''

pitcher_df['player_pos'] = 'pitcher'
pitcher_df['sport'] = 'Baseball'
pitcher_df['league'] = 'MLB'
pitcher_df['timestamp'] = datetime.now().isoformat(timespec='seconds')

for col in ['bet_type', 'prop_correct', 'book', 'price']:
    if col not in pitcher_df.columns:
        pitcher_df[col] = ''

if 'game_id' in pitcher_df.columns:
    try:
        pitcher_df['game_id'] = (
            pd.to_numeric(pitcher_df['game_id'], errors='coerce')
            .astype('Int64')
            .astype(str)
        )
        pitcher_df.loc[pitcher_df['game_id'] == '<NA>', 'game_id'] = ''
    except Exception:
        pitcher_df['game_id'] = pitcher_df['game_id'].astype(str).replace({'nan': ''})

# -------- FINAL STEP: Keep only pitchers who are in today's normalized games --------
try:
    tg = pd.read_csv(todays_games_file)
    # Ensure required columns exist
    for col in ['pitcher_home', 'pitcher_away']:
        if col not in tg.columns:
            tg[col] = ''
    # Normalize names for matching (case-insensitive, trimmed)
    def _norm(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.casefold()

    todays_pitchers = set(_norm(tg['pitcher_home
