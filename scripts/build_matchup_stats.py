import pandas as pd

# Input paths
BATTERS_HOME_FILE = "data/adjusted/batters_home_weather_park.csv"
BATTERS_AWAY_FILE = "data/adjusted/batters_away_weather_park.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home_weather_park.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away_weather_park.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"

# Output path
OUTPUT_FILE = "data/final/matchup_stats.csv"

def validate_required_columns(df, required_cols, filename):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {filename}: {missing}")

def get_pitcher_woba(df, team_col, name_col):
    return df[[team_col, name_col, "adj_woba_combined"]].drop_duplicates(subset=[team_col, name_col])

def build_matchup_df(bh, ba, ph, pa, games):
    validate_required_columns(games, ["home_team", "away_team", "pitcher_home", "pitcher_away"], "games")
    validate_required_columns(bh, ["name", "adj_woba_combined", "home_team_park"], "batters_home")
    validate_required_columns(ba, ["name", "adj_woba_combined", "away_team"], "batters_away")
    validate_required_columns(ph, ["name", "adj_woba_combined", "home_team"], "pitchers_home")
    validate_required_columns(pa, ["name", "adj_woba_combined", "away_team"], "pitchers_away")

    bh = bh.merge(
        games[["home_team", "pitcher_home"]],
        how="left",
        left_on="home_team_park",
        right_on="home_team"
    )

    ba = ba.merge(
        games[["away_team", "pitcher_away"]],
        how="left",
        on="away_team"
    )

    home_pitcher_stats = get_pitcher_woba(ph, "home_team", "name")
    away_pitcher_stats = get_pitcher_woba(pa, "away_team", "name")

    bh = bh.merge(
        home_pitcher_stats,
        how="left",
        left_on=["home_team", "pitcher_home"],
        right_on=["home_team", "name"],
        suffixes=("", "_pitcher")
    )

    ba = ba.merge(
        away_pitcher_stats,
        how="left",
        left_on=["away_team", "pitcher_away"],
        right_on=["away_team", "name"],
        suffixes=("", "_pitcher")
    )

    bh["side"] = "home"
    ba["side"] = "away"

    return pd.concat([bh, ba], ignore_index=True)

def main():
    bh = pd.read_csv(BATTERS_HOME_FILE)
    ba = pd.read_csv(BATTERS_AWAY_FILE)
    ph = pd.read_csv(PITCHERS_HOME_FILE)
    pa = pd.read_csv(PITCHERS_AWAY_FILE)
    games = pd.read_csv(GAMES_FILE)

    matchup_stats = build_matchup_df(bh, ba, ph, pa, games)
    matchup_stats.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
