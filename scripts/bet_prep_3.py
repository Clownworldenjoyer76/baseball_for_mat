# scripts/bet_prep_3.py
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

def _norm_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _norm_key(s: pd.Series) -> pd.Series:
    # aggressive key for joining different sources
    return s.astype(str).str.strip().str.lower()

def _safe_int_str(s: pd.Series) -> pd.Series:
    try:
        out = pd.to_numeric(s, errors='coerce').astype('Int64').astype(str)
        return out.mask(out.eq('<NA>'), '')
    except Exception:
        return s.astype(str).replace({'nan': ''})

try:
    mlb_sched_df = pd.read_csv(sched_file)
    pitcher_df   = pd.read_csv(pitcher_props_file)
    team_map_df  = pd.read_csv(team_map_file)
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure input files are in the correct directory.")
    raise SystemExit(1)

# -------- Normalize team names via team_name_master --------
team_map_df['clean_team_name'] = _norm_key(team_map_df['clean_team_name'])
team_map_df['team_name']       = _norm_str(team_map_df['team_name'])

if 'team' not in pitcher_df.columns:
    pitcher_df['team'] = ''

pitcher_df['_team_norm'] = _norm_key(pitcher_df['team'])
pitcher_df = pitcher_df.merge(
    team_map_df[['clean_team_name', 'team_name']].rename(
        columns={'clean_team_name': '_team_norm', 'team_name': '_team_mapped'}
    ),
    on='_team_norm', how='left'
)
pitcher_df['team'] = pitcher_df['_team_mapped'].fillna(pitcher_df['team'])
pitcher_df.drop(columns=['_team_norm', '_team_mapped'], errors='ignore', inplace=True)

# -------- Merge date/game_id from schedule (both home/away) --------
mlb_sched_df['home_team'] = _norm_str(mlb_sched_df['home_team'])
mlb_sched_df['away_team'] = _norm_str(mlb_sched_df['away_team'])

mlb_sched_away = mlb_sched_df.rename(columns={'away_team': 'team'})[['team','date','game_id']]
mlb_sched_home = mlb_sched_df.rename(columns={'home_team': 'team'})[['team','date','game_id']]
mlb_sched_merged = pd.concat([mlb_sched_away, mlb_sched_home], ignore_index=True)
mlb_sched_merged['team'] = _norm_str(mlb_sched_merged['team'])

pitcher_df = pd.merge(pitcher_df, mlb_sched_merged[['team','date','game_id']], on='team', how='left')

# -------- Add / Rename / Defaults --------
pitcher_df['player']     = pitcher_df.get('name', '')
if 'prop_type' in pitcher_df.columns and 'prop' not in pitcher_df.columns:
    pitcher_df.rename(columns={'prop_type': 'prop'}, inplace=True)
elif 'prop' not in pitcher_df.columns:
    pitcher_df['prop'] = ''  # not strictly needed downstream, but keep for consistency

pitcher_df['player_pos'] = 'pitcher'
pitcher_df['sport']      = 'Baseball'
pitcher_df['league']     = 'MLB'
pitcher_df['timestamp']  = datetime.now().isoformat(timespec='seconds')

for col in ['bet_type','prop_correct','book','price']:
    if col not in pitcher_df.columns:
        pitcher_df[col] = ''

if 'game_id' in pitcher_df.columns:
    pitcher_df['game_id'] = _safe_int_str(pitcher_df['game_id'])

# -------- Determine today's teams from schedule --------
today_teams = set(_norm_str(mlb_sched_merged['team']).unique())

# -------- Prefer by-name filter if todaysgames file exists; fallback by team --------
name_matched = pd.DataFrame()
fallback = pd.DataFrame()
missing_teams = []

try:
    tg = pd.read_csv(todays_games_file)
    for c in ['pitcher_home','pitcher_away','home_team','away_team']:
        if c not in tg.columns:
            tg[c] = ''
    tg_pitchers = set(_norm_key(tg['pitcher_home']).tolist() + _norm_key(tg['pitcher_away']).tolist())

    pitcher_df['_name_norm'] = _norm_key(pitcher_df['name'])
    by_name = pitcher_df[pitcher_df['_name_norm'].isin(tg_pitchers)].copy()
    name_matched = by_name.copy()

    # For any team that didn't get a name match but is on today's schedule, keep best available row
    have_by_name = set(_norm_str(by_name['team']).unique())
    need_fallback = sorted(t for t in today_teams if t not in have_by_name)

    if 'mega_z' in pitcher_df.columns:
        chooser = pitcher_df.sort_values(['team','mega_z'], ascending=[True, False]).groupby('team', as_index=False).head(1)
    else:
        chooser = pitcher_df.groupby('team', as_index=False).head(1)

    chooser['_team_norm_pick'] = _norm_str(chooser['team'])
    fb = chooser[chooser['_team_norm_pick'].isin(need_fallback)].copy()
    fallback = fb.copy()
    selected = pd.concat([by_name, fb], ignore_index=True)
    selected.drop(columns=['_name_norm','_team_norm_pick'], errors='ignore', inplace=True)

    # Ensure we only keep one row per team among today's teams
    selected['_team_norm'] = _norm_str(selected['team'])
    selected = selected[selected['_team_norm'].isin(today_teams)].copy()
    selected = selected.sort_values(['_team_norm', 'date']).groupby('_team_norm', as_index=False).head(1)
    selected.drop(columns=['_team_norm'], inplace=True, errors='ignore')
    pitcher_df = selected

    missing_teams = [t for t in today_teams if t not in set(_norm_str(pitcher_df['team']).unique())]

except FileNotFoundError:
    print(f"⚠️ {todays_games_file} not found. Falling back to best-by-team from projections for scheduled teams.")
    # No by-name filtering; just pick best per team for teams playing today
    pool = pitcher_df.copy()
    pool['_team_norm'] = _norm_str(pool['team'])
    pool = pool[pool['_team_norm'].isin(today_teams)]
    if 'mega_z' in pool.columns:
        pitcher_df = pool.sort_values(['_team_norm','mega_z'], ascending=[True, False]).groupby('_team_norm', as_index=False).head(1)
    else:
        pitcher_df = pool.groupby('_team_norm', as_index=False).head(1)
    pitcher_df.drop(columns=['_team_norm'], inplace=True, errors='ignore')

# -------- Save --------
pitcher_df.to_csv(output_file, index=False)
print(f"✅ Saved -> {output_file} (rows: {len(pitcher_df)})")

# -------- Logs for visibility --------
if not name_matched.empty:
    by_name_counts = _norm_str(name_matched['team']).value_counts().to_dict()
    print(f"🧾 Name-matched teams: {len(by_name_counts)} | {by_name_counts}")
if not fallback.empty:
    fb_counts = _norm_str(fallback['team']).value_counts().to_dict()
    print(f"🔁 Fallback-by-team used for: {len(fb_counts)} team(s) | {fb_counts}")
if 'missing_teams' in locals() and missing_teams:
    print(f"❗ Still missing after fallback: {sorted(missing_teams)}")
