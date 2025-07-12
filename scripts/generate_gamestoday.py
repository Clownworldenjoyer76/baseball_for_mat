import pandas as pd
from datetime import datetime

# Load input files
games_df = pd.read_csv('data/raw/todaysgames.csv')
lineups_df = pd.read_csv('data/raw/lineups.csv')
team_map_df = pd.read_csv('data/Data/team_name_map.csv')
stadium_df = pd.read_csv('data/Data/stadium_metadata.csv')

# Create team name mapping
team_map = dict(zip(team_map_df['name'], team_map_df['team']))

# Normalize team names in games_df
games_df['home_team'] = games_df['home_team'].map(team_map)
games_df['away_team'] = games_df['away_team'].map(team_map)

# Normalize team names in lineups_df
lineups_df['team'] = lineups_df['team code'].map(team_map)

# Get lineups grouped by team
lineup_groups = lineups_df.groupby('team')['batter_name'].apply(list).to_dict()

# Extract pitchers (batting_order == 0)
pitchers = lineups_df[lineups_df['batting_order'] == 0][['team', 'batter_name']].set_index('team').to_dict()['batter_name']

# Prepare output
output = []

for _, row in games_df.iterrows():
    home = row['home_team']
    away = row['away_team']
    time = row['game_time']
    venue_row = stadium_df[stadium_df['home_team'] == home]
    venue = venue_row['venue'].values[0] if not venue_row.empty else ''

    output.append({
        'game_time': time,
        'away_team': away,
        'away_pitcher': pitchers.get(away, ''),
        'home_team': home,
        'home_pitcher': pitchers.get(home, ''),
        'venue': venue,
        'away_lineup': ', '.join(lineup_groups.get(away, [])),
        'home_lineup': ', '.join(lineup_groups.get(home, []))
    })

# Save output
output_df = pd.DataFrame(output)
output_df.to_csv('data/daily/games_today.csv', index=False)
