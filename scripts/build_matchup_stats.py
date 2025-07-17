import pandas as pd

# Input paths
BATTERS_HOME_FILE = "data/adjusted/batters_home_weather_park.csv"
BATTERS_AWAY_FILE = "data/adjusted/batters_away_weather_park.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home_weather_park.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away_weather_park.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"

# Output path
OUTPUT_FILE = "data/final/matchup_stats.csv"

REQUIRED_GAME_COLUMNS = ["home_team", "away_team", "home_pitcher", "away_pitcher"]

def validate_required_columns(df, required_cols, filename):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {filename}: {missing}")

def get_pitcher_woba(df, team_col, name_col):
    validate_required_columns(df, [team_col, name_col, "adj_woba_combined"], "pitcher file")
    return df[[team_col, name_col, "adj_woba_combined"]].drop_duplicates(subset=[team_col, name_col])

def build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games):
    validate_required_columns(games, REQUIRED_GAME_COLUMNS, "todaysgames_normalized.csv")

    batters_home = batters_home.merge(
        games[["home_team", "home_pitcher"]],
        how="left",
        on="home_team"
    )
    batters_away = batters_away.merge(
        games[["away_team", "away_pitcher"]],
        how="left",
        on="away_team"
    )

    home_pitcher_stats = get_pitcher_woba(pitchers_home, "home_team", "name")
    away_pitcher_stats = get_pitcher_woba(pitchers_away, "away_team", "name")

    batters_home = batters_home.merge(
        home_pitcher_stats,
        how="left",
        left_on=["home_team", "home_pitcher"],
        right_on=["home_team", "name"],
        suffixes=("", "_pitcher")
    )
    batters_away = batters_away.merge(
        away_pitcher_stats,
        how="left",
        left_on=["away_team", "away_pitcher"],
        right_on=["away_team", "name"],
        suffixes=("", "_pitcher")
    )

    batters_home["side"] = "home"
    batters_away["side"] = "away"

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

if __name__ == "__main__":
    main()
