import pandas as pd
from utils import load_csv, normalize_name

def main():
    games_path = 'data/raw/todaysgames.csv'
    lineups_path = 'data/raw/lineups.csv'
    team_map_path = 'data/Data/team_name_map.csv'
    stadiums_path = 'data/Data/stadium_metadata.csv'

    games = load_csv(games_path)
    lineups = load_csv(lineups_path)
    team_map = load_csv(team_map_path)
    stadiums = load_csv(stadiums_path)

    team_map['name'] = team_map['name'].apply(normalize_name)
    team_map['team'] = team_map['team'].apply(normalize_name)
    name_to_team = dict(zip(team_map['name'], team_map['team']))
    standard_to_name = dict(zip(team_map['team'], team_map['name']))

    games['home_team'] = games['home_team'].apply(normalize_name)
    games['away_team'] = games['away_team'].apply(normalize_name)

    games['home_team'] = games['home_team'].map(name_to_team)
    games['away_team'] = games['away_team'].map(name_to_team)

    results = []

    for _, row in games.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']

        venue = stadiums[stadiums['home_team'] == home_team]['venue'].values[0] if home_team in stadiums['home_team'].values else None

        results.append({
            'home_team': home_team,
            'away_team': away_team,
            'venue': venue
        })

    output_df = pd.DataFrame(results)
    output_df.to_csv('data/daily/games_today_output.csv', index=False)

if __name__ == "__main__":
    main()