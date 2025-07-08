
import os
import pandas as pd
import unicodedata
import re

def normalize_name(name):
    name = name.lower()
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\b(jr|ii|iii)\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def load_team_files(directory):
    team_map = {}
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            team_name = filename.split('_')[1].replace('.csv', '')
            df = pd.read_csv(os.path.join(directory, filename))
            if 'name' in df.columns:
                df['normalized_name'] = df['name'].apply(normalize_name)
            elif 'last_name, first_name' in df.columns:
                df['normalized_name'] = df['last_name, first_name'].apply(normalize_name)
            for norm_name in df['normalized_name']:
                team_map[norm_name] = team_name
    return team_map

def tag_players(input_file, col_name, team_map, output_file, unmatched_file):
    df = pd.read_csv(input_file)
    df['normalized_name'] = df[col_name].apply(normalize_name)
    df['team'] = df['normalized_name'].map(team_map)
    matched = df[df['team'].notna()].drop(columns=['normalized_name'])
    unmatched = df[df['team'].isna()].drop(columns=['normalized_name'])
    matched.to_csv(output_file, index=False)
    unmatched.to_csv(unmatched_file, index=False)
    return len(matched), len(unmatched)

# Paths
team_dir = 'data/team_csvs'
batter_input = 'data/master/batters.csv'
pitcher_input = 'data/master/pitchers.csv'
output_dir = 'data/tagged'
os.makedirs(output_dir, exist_ok=True)

# Build team map
team_map = load_team_files(team_dir)

# Process batters
bat_out = os.path.join(output_dir, 'batters_tagged.csv')
bat_unmatched = os.path.join(output_dir, 'unmatched_batters.csv')
bat_matched, bat_unmatched_count = tag_players(batter_input, 'name', team_map, bat_out, bat_unmatched)

# Process pitchers
pitch_out = os.path.join(output_dir, 'pitchers_tagged.csv')
pitch_unmatched = os.path.join(output_dir, 'unmatched_pitchers.csv')
pitch_matched, pitch_unmatched_count = tag_players(pitcher_input, 'last_name, first_name', team_map, pitch_out, pitch_unmatched)

# Write totals
output_summary = f"""Total batters in CSV: {bat_matched + bat_unmatched_count}
Matched batters: {bat_matched}
Unmatched batters: {bat_unmatched_count}

Total pitchers in CSV: {pitch_matched + pitch_unmatched_count}
Matched pitchers: {pitch_matched}
Unmatched pitchers: {pitch_unmatched_count}
"""
with open('data/output/player_totals.txt', 'w') as f:
    f.write(output_summary)

print(f"✅ batters_tagged.csv created with {bat_matched} rows")
if bat_unmatched_count > 0:
    print(f"⚠️ Unmatched batters (missing team): {bat_unmatched_count} written to unmatched_batters.csv")

print(f"✅ pitchers_tagged.csv created with {pitch_matched} rows")
if pitch_unmatched_count > 0:
    print(f"⚠️ Unmatched pitchers (missing team): {pitch_unmatched_count} written to unmatched_pitchers.csv")

print("📄 Totals written to player_totals.txt")
