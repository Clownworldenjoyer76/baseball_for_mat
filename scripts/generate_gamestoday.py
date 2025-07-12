import pandas as pd
import os

def load_csv(path):
    return pd.read_csv(path)

def normalize_name(name):
    parts = name.strip().split()
    if len(parts) == 1:
        return parts[0]
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

def main():
    games_path = 'data/raw/todaysgames.csv'
    lineups_path = 'data/raw/lineups.csv'
    team_map_path = 'data/Data/team_name_map.csv'
    batters_path = 'data/cleaned/batters_normalized_cleaned.csv'
    pitchers_path = 'data/cleaned/pitchers_normalized_cleaned.csv'
    stadiums_path = 'data/Data/stadium_metadata.csv'

    games = load_csv(games_path)
    lineups = load_csv(lineups_path)
    team_map = load_csv(team_map_path)
    batters = load_csv(batters_path)
    pitchers = load_csv(pitchers_path)
    stadiums = load_csv(stadiums_path)

    # Normalize team names
    team_map_dict = dict(zip(team_map['original'], team_map['standard']))
    games['home_team'] = games['home_team'].map(team_map_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].map(team_map_dict).fillna(games['away_team'])

    merged_data = []

    for _, row in games.iterrows():
        home = row['home_team']
        away = row['away_team']
        time = row['game_time']

        home_pitcher = pitchers[pitchers['team'] == home].head(1)['name'].values
        away_pitcher = pitchers[pitchers['team'] == away].head(1)['name'].values

        venue_row = stadiums[stadiums['team'] == home]
        venue = venue_row['stadium'].values[0] if not venue_row.empty else 'Unknown'

        home_lineup = lineups[(lineups['team'] == home)].head(9)['player'].tolist()
        away_lineup = lineups[(lineups['team'] == away)].head(9)['player'].tolist()

        merged_data.append({
            'game_time': time,
            'home_team': home,
            'away_team': away,
            'home_pitcher': normalize_name(home_pitcher[0]) if len(home_pitcher) > 0 else '',
            'away_pitcher': normalize_name(away_pitcher[0]) if len(away_pitcher) > 0 else '',
            'venue': venue,
            'home_lineup': ';'.join([normalize_name(p) for p in home_lineup]),
            'away_lineup': ';'.join([normalize_name(p) for p in away_lineup])
        })

    final_df = pd.DataFrame(merged_data)
    os.makedirs('data/daily', exist_ok=True)
    final_df.to_csv('data/daily/gamestoday.csv', index=False)

if __name__ == "__main__":
    main()
