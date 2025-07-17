import pandas as pd

# Input paths
BATTERS_HOME_FILE = "data/adjusted/batters_home_weather_park.csv"
BATTERS_AWAY_FILE = "data/adjusted/batters_away_weather_park.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home_weather_park.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away_weather_park.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"

# Output path
OUTPUT_FILE = "data/final/matchup_stats.csv"

def get_pitcher_woba(df, team_col, name_col):
    return df[[team_col, name_col, "adj_woba_combined"]].drop_duplicates(subset=[team_col, name_col])

def build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games):
    # Merge team and pitcher name from games into batter files
    batters_home = batters_home.merge(games[["home_team", "home_pitcher"]], on="home_team", how="left")
    batters_away = batters_away.merge(games[["away_team", "away_pitcher"]], on="away_team", how="left")

    # Get relevant pitcher wOBA values
    home_pitcher_stats = get_pitcher_woba(pitchers_home, "home_team", "name")
    away_pitcher_stats = get_pitcher_woba(pitchers_away, "away_team", "name")

    # Join pitcher stats into batter data
    batters_home = batters_home.merge(home_pitcher_stats, left_on=["home_team", "home_pitcher"],
                                      right_on=["home_team", "name"], how="left", suffixes=("", "_pitcher"))
    batters_away = batters_away.merge(away_pitcher_stats, left_on=["away_team", "away_pitcher"],
                                      right_on=["away_team", "name"], how="left", suffixes=("", "_pitcher"))

    # Add tag
    batters_home["side"] = "home"
    batters_away["side"] = "away"

    # Combine
    combined = pd.concat([batters_home, batters_away], ignore_index=True)
    return combined

def main():
    batters_home = pd.read_csv(BATTERS_HOME_FILE)
    batters_away = pd.read_csv(BATTERS_AWAY_FILE)
    pitchers_home = pd.read_csv(PITCHERS_HOME_FILE)
    pitchers_away = pd.read_csv(PITCHERS_AWAY_FILE)
    games = pd.read_csv(GAMES_FILE)

    matchup_stats = build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games)
    matchup_stats.to_csv(OUTPUT_FILE, index=False)

main()
