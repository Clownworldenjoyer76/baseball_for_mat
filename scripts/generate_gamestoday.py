import pandas as pd
import os

def load_csv(path):
    return pd.read_csv(path)

def normalize_team_name(name, team_map):
    if name in team_map:
        return team_map[name]
    return name

def format_player_name(name):
    parts = name.strip().split()
    if len(parts) < 2:
        return name
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

def main():
    todaysgames_path = "data/raw/todaysgames.csv"
    lineups_path = "data/raw/lineups.csv"
    team_map_path = "data/Data/team_name_map.csv"
    stadium_path = "data/Data/stadium_metadata.csv"

    todaysgames = load_csv(todaysgames_path)
    lineups = load_csv(lineups_path)
    team_map_df = load_csv(team_map_path)
    stadiums = load_csv(stadium_path)

    team_map = dict(zip(team_map_df['name'], team_map_df['team']))

    data = []
    for _, row in todaysgames.iterrows():
        home_team_raw = row['home_team']
        away_team_raw = row['away_team']
        game_time = row['game_time']

        home_team = normalize_team_name(home_team_raw, team_map)
        away_team = normalize_team_name(away_team_raw, team_map)

        # Get venue from stadium metadata
        venue_row = stadiums[stadiums['team'] == home_team]
        venue = venue_row['stadium'].values[0] if not venue_row.empty else ""

        # Get lineups
        home_lineup = lineups[(lineups['team code'] == home_team)].head(9)['player name'].apply(format_player_name).tolist()
        away_lineup = lineups[(lineups['team code'] == away_team)].head(9)['player name'].apply(format_player_name).tolist()

        data.append({
            'game_time': game_time,
            'home_team': home_team,
            'away_team': away_team,
            'venue': venue,
            'home_pitcher': '',  # Placeholder
            'away_pitcher': '',  # Placeholder
            'home_lineup': ';'.join(home_lineup),
            'away_lineup': ';'.join(away_lineup)
        })

    gamestoday_df = pd.DataFrame(data)
    gamestoday_df.to_csv("data/daily/gamestoday.csv", index=False)

if __name__ == "__main__":
    main()
