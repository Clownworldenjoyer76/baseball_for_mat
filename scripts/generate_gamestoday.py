
import pandas as pd
from scripts.utils import load_csv, normalize_name

def main():
    # Load files
    games = load_csv('data/raw/todaysgames.csv')
    lineups = load_csv('data/raw/lineups.csv')
    batters = load_csv('data/cleaned/batters_normalized_cleaned.csv')
    pitchers = load_csv('data/cleaned/pitchers_normalized_cleaned.csv')
    team_map = load_csv('data/Data/team_name_map.csv')
    stadiums = load_csv('data/Data/stadium_metadata.csv')

    # Normalize team names
    team_dict = dict(zip(team_map['name'], team_map['team']))
    games['away_team'] = games['away_team'].map(team_dict)
    games['home_team'] = games['home_team'].map(team_dict)
    lineups['team'] = lineups['team code'].map(team_dict)

    output_rows = []

    for _, row in games.iterrows():
        away_team = row['away_team']
        home_team = row['home_team']
        game_time = row['game_time']

        away_pitcher = normalize_name(row['away_pitcher'])
        home_pitcher = normalize_name(row['home_pitcher'])

        # Get venue from corrected column
        venue = stadiums[stadiums['home_team'] == home_team]['venue'].values[0]

        # Get starting lineups
        away_lineup = lineups[(lineups['team'] == away_team) & (lineups['starting'] == True)]['player'].tolist()[:9]
        home_lineup = lineups[(lineups['team'] == home_team) & (lineups['starting'] == True)]['player'].tolist()[:9]

        output_rows.append({
            'game_time': game_time,
            'away_team': away_team,
            'away_pitcher': away_pitcher,
            'home_team': home_team,
            'home_pitcher': home_pitcher,
            'venue': venue,
            'away_lineup': ';'.join(away_lineup),
            'home_lineup': ';'.join(home_lineup)
        })

    df = pd.DataFrame(output_rows)
    df.to_csv('data/daily/gamestoday.csv', index=False)

if __name__ == "__main__":
    main()
