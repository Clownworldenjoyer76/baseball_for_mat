import pandas as pd
import os

def load_csv(path):
    return pd.read_csv(path)

def normalize_name(name):
    parts = name.strip().split()
    if len(parts) < 2:
        return name
    return f"{parts[-1].capitalize()}, {' '.join(parts[:-1]).capitalize()}"

def normalize_team(team, team_map):
    return team_map.get(team.strip(), team.strip())

def main():
    # File paths
    base_path = "data"
    todaysgames_path = os.path.join(base_path, "daily", "todaysgames.csv")
    lineups_path = os.path.join(base_path, "daily", "lineups.csv")
    team_map_path = os.path.join(base_path, "Data", "team_name_map.csv")
    stadium_path = os.path.join(base_path, "Data", "stadium_metadata.csv")

    # Load files
    games = load_csv(todaysgames_path)
    lineups = load_csv(lineups_path)
    team_map_df = load_csv(team_map_path)
    stadium_df = load_csv(stadium_path)

    # Create mapping dictionaries
    team_map = dict(zip(team_map_df['input'], team_map_df['standard']))
    venue_map = dict(zip(stadium_df['team'], stadium_df['stadium']))

    # Normalize teams
    games['away_team'] = games['away_team'].apply(lambda x: normalize_team(x, team_map))
    games['home_team'] = games['home_team'].apply(lambda x: normalize_team(x, team_map))
    games['venue'] = games['home_team'].map(venue_map)

    # Normalize lineup
    lineups['team'] = lineups['team'].apply(lambda x: normalize_team(x, team_map))
    lineups['batter_name'] = lineups['batter_name'].apply(normalize_name)

    # Merge lineups into one string per team
    merged_lineups = (
        lineups.groupby(['team', 'game_time'])['batter_name']
        .apply(lambda x: ', '.join(x.head(9)))
        .reset_index()
        .rename(columns={'batter_name': 'starting_lineup'})
    )

    # Merge lineups with game data
    full_df = pd.merge(games, merged_lineups, left_on=['home_team', 'game_time'], right_on=['team', 'game_time'], how='left')
    full_df = full_df.rename(columns={'starting_lineup': 'home_lineup'}).drop(columns=['team'])

    full_df = pd.merge(full_df, merged_lineups, left_on=['away_team', 'game_time'], right_on=['team', 'game_time'], how='left')
    full_df = full_df.rename(columns={'starting_lineup': 'away_lineup'}).drop(columns=['team'])

    # Save to final file
    full_df[['game_time', 'away_team', 'home_team', 'venue', 'home_lineup', 'away_lineup']].to_csv(os.path.join(base_path, "daily", "gamestoday.csv"), index=False)

if __name__ == "__main__":
    main()
