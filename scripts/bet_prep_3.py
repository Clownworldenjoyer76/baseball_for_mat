# bet_prep_3.py (updated)
import pandas as pd
from datetime import datetime
import os

# -------- Config --------
sched_file = 'data/bets/mlb_sched.csv'
pitcher_props_file = 'data/_projections/pitcher_mega_z.csv'
output_dir = 'data/bets/prep'
output_file = os.path.join(output_dir, 'pitcher_props_bets.csv')

# 30-team alias map (short/nicknames -> schedule full names)
TEAM_ALIASES = {
    # AL East
    "yankees": "New York Yankees",
    "nyy": "New York Yankees",
    "red sox": "Boston Red Sox",
    "bos": "Boston Red Sox",
    "blue jays": "Toronto Blue Jays",
    "jays": "Toronto Blue Jays",
    "tor": "Toronto Blue Jays",
    "rays": "Tampa Bay Rays",
    "tb": "Tampa Bay Rays",
    "orioles": "Baltimore Orioles",
    "bal": "Baltimore Orioles",

    # AL Central
    "guardians": "Cleveland Guardians",
    "cle": "Cleveland Guardians",
    "tigers": "Detroit Tigers",
    "det": "Detroit Tigers",
    "royals": "Kansas City Royals",
    "kc": "Kansas City Royals",
    "twins": "Minnesota Twins",
    "min": "Minnesota Twins",
    "white sox": "Chicago White Sox",
    "cws": "Chicago White Sox",

    # AL West
    "astros": "Houston Astros",
    "hou": "Houston Astros",
    "angels": "Los Angeles Angels",
    "laa": "Los Angeles Angels",
    "mariners": "Seattle Mariners",
    "sea": "Seattle Mariners",
    # MLB now lists simply "Athletics"
    "athletics": "Athletics",
    "oakland athletics": "Athletics",
    "oak": "Athletics",
    "rangers": "Texas Rangers",
    "tex": "Texas Rangers",

    # NL East
    "mets": "New York Mets",
    "nym": "New York Mets",
    "braves": "Atlanta Braves",
    "atl": "Atlanta Braves",
    "phillies": "Philadelphia Phillies",
    "phi": "Philadelphia Phillies",
    "nationals": "Washington Nationals",
    "was": "Washington Nationals",
    "marlins": "Miami Marlins",
    "mia": "Miami Marlins",

    # NL Central
    "cubs": "Chicago Cubs",
    "chc": "Chicago Cubs",
    "cardinals": "St. Louis Cardinals",
    "stl": "St. Louis Cardinals",
    "brewers": "Milwaukee Brewers",
    "mil": "Milwaukee Brewers",
    "pirates": "Pittsburgh Pirates",
    "pit": "Pittsburgh Pirates",
    "reds": "Cincinnati Reds",
    "cin": "Cincinnati Reds",

    # NL West
    "dodgers": "Los Angeles Dodgers",
    "lad": "Los Angeles Dodgers",
    "giants": "San Francisco Giants",
    "sf": "San Francisco Giants",
    "padres": "San Diego Padres",
    "sd": "San Diego Padres",
    "rockies": "Colorado Rockies",
    "col": "Colorado Rockies",
    "diamondbacks": "Arizona Diamondbacks",
    "dbacks": "Arizona Diamondbacks",
    "ari": "Arizona Diamondbacks",
}

def norm_team_to_sched_name(s: str) -> str:
    """Normalize a team label to the full schedule name using TEAM_ALIASES.
    If no alias match, return the original trimmed string.
    """
    if pd.isna(s):
        return s
    t = str(s).strip()
    key = t.lower()
    return TEAM_ALIASES.get(key, t)

# -------- IO --------
os.makedirs(output_dir, exist_ok=True)

try:
    mlb_sched_df = pd.read_csv(sched_file)
    pitcher_df = pd.read_csv(pitcher_props_file)
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure input files are in the correct directory.")
    raise SystemExit(1)

# Trim/normalize schedule team labels
for col in ("home_team", "away_team"):
    if col in mlb_sched_df.columns:
        mlb_sched_df[col] = mlb_sched_df[col].astype(str).str.strip()

# Prepare schedule rows with a unified 'team' column for merging
mlb_sched_away = mlb_sched_df.rename(columns={'away_team': 'team'})
mlb_sched_home = mlb_sched_df.rename(columns={'home_team': 'team'})
mlb_sched_merged = pd.concat([mlb_sched_away, mlb_sched_home], ignore_index=True)

# Normalize 'team' in both DataFrames to schedule naming
pitcher_df['team'] = pitcher_df['team'].apply(norm_team_to_sched_name)
mlb_sched_merged['team'] = mlb_sched_merged['team'].astype(str).str.strip()

# --- Merge date and game_id from schedule ---
pitcher_df = pd.merge(
    pitcher_df,
    mlb_sched_merged[['team', 'date', 'game_id']],
    on='team',
    how='left'
)

# --- Add, Rename, and Modify Columns ---
# Copy "name" -> "player"
pitcher_df['player'] = pitcher_df.get('name', '')

# Ensure 'prop' column exists (some inputs may use 'prop_type')
if 'prop_type' in pitcher_df.columns and 'prop' not in pitcher_df.columns:
    pitcher_df.rename(columns={'prop_type': 'prop'}, inplace=True)
elif 'prop' not in pitcher_df.columns:
    pitcher_df['prop'] = ''

# Static fields
pitcher_df['player_pos'] = 'pitcher'
pitcher_df['sport'] = 'Baseball'   # << requested: capitalized
pitcher_df['league'] = 'MLB'
pitcher_df['timestamp'] = datetime.now().isoformat(timespec='seconds')

# Ensure optional columns exist
for col in ['bet_type', 'prop_correct', 'book', 'price']:
    if col not in pitcher_df.columns:
        pitcher_df[col] = ''

# Cast game_id to string while preserving blanks
if 'game_id' in pitcher_df.columns:
    # Use pandas nullable Int to avoid 'nan' strings, then to str
    try:
        pitcher_df['game_id'] = pd.to_numeric(pitcher_df['game_id'], errors='coerce').astype('Int64').astype(str)
        pitcher_df.loc[pitcher_df['game_id'] == '<NA>', 'game_id'] = ''
    except Exception:
        # Fallback: keep as string but replace 'nan' with empty
        pitcher_df['game_id'] = pitcher_df['game_id'].astype(str).replace({'nan': ''})

# Save
pitcher_df.to_csv(output_file, index=False)
print(f"Saved -> {output_file}")
