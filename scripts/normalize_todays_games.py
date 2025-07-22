import pandas as pd
import subprocess

# Load files
games = pd.read_csv('data/raw/todaysgames.csv')
team_map = pd.read_csv('data/Data/team_abv_map.csv')
pitchers = pd.read_csv('data/cleaned/pitchers_normalized_cleaned.csv')

# Normalize team names
team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

# Normalize pitcher names
def normalize_name(name):
    name = str(name).strip().replace(".", "")
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

games['pitcher_home'] = games['pitcher_home'].apply(normalize_name)
games['pitcher_away'] = games['pitcher_away'].apply(normalize_name)

# Allow pitchers to be in valid list OR equal to 'Undecided'
valid_pitchers = set(pitchers['last_name, first_name'])
games = games[
    (games['pitcher_home'].isin(valid_pitchers) | (games['pitcher_home'] == 'Undecided')) &
    (games['pitcher_away'].isin(valid_pitchers) | (games['pitcher_away'] == 'Undecided'))
]

# Save normalized file
output_file = 'data/raw/todaysgames_normalized.csv'
games.to_csv(output_file, index=False)
print(f"✅ normalize_todays_games.py completed. Output saved to {output_file}")

# Git commit + push
try:
    subprocess.run(["git", "add", output_file], check=True)
    subprocess.run(["git", "commit", "-m", "🔄 Update todaysgames_normalized.csv after normalization"], check=True)
    subprocess.run(["git", "push"], check=True)
    print("✅ Git commit and push complete.")
except subprocess.CalledProcessError as e:
    print(f"⚠️ Git commit/push failed: {e}")
